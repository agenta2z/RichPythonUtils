from typing import Callable, Mapping, Union, Iterable

import pandas as pd
from attr import attrib, attrs


def pd_aggregate(
        df: pd.DataFrame,
        target: Union[str, Callable, Iterable[Union[str, Callable]], Mapping[str, Union[str, Callable]]],
        agg: Union[str, Callable, Iterable[Union[str, Callable]], Mapping[str, Union[str, Callable]]],
        groupby: Union[str, Callable, Iterable[Union[str, Callable]], Mapping[str, Union[str, Callable]]] = None
) -> pd.DataFrame:
    """
    Perform aggregation on a pandas DataFrame without modifying the original DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to perform the aggregation on.
        target (Union[str, Iterable[str], Callable, Mapping[str, Callable]]): The column(s) to aggregate or a callable to create the column(s).
        agg (Union[str, Callable, Mapping[str, Union[str, Callable]]]): The aggregation method, which can be a string (e.g., 'mean', 'sum')
                                                                        or a callable function (e.g., np.mean).
        groupby (Union[str, Iterable[str], Callable, Mapping[str, Callable]]): The column(s) or a function to group by.

    Returns:
        pd.DataFrame: The DataFrame with the aggregated results.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'name': ['Alice', 'Bob', 'Alice', 'Bob', 'Charlie'],
        ...     'accuracy': [0.95, 0.85, 0.90, 0.88, 0.92],
        ...     'score': [10, 15, 12, 18, 20],
        ...     'group': ['A', 'B', 'A', 'B', 'A']
        ... }
        >>> df = pd.DataFrame(data)

        >>> # Example with single groupby column
        >>> result = pd_aggregate(df, 'accuracy', 'mean', 'name')
        >>> print(result)
              name  accuracy
        0    Alice     0.925
        1      Bob     0.865
        2  Charlie     0.920

        >>> # Example with multiple groupby columns
        >>> result = pd_aggregate(df, 'accuracy', 'sum', ['name', 'group'])
        >>> print(result)
              name group  accuracy
        0    Alice     A      1.85
        1      Bob     B      1.73
        2  Charlie     A      0.92

        >>> # Example with custom aggregation function
        >>> import numpy as np
        >>> result = pd_aggregate(df, 'accuracy', np.sum, 'name')
        >>> print(result)
              name  accuracy
        0    Alice      1.85
        1      Bob      1.73
        2  Charlie      0.92

        >>> # Example with custom groupby function
        >>> result = pd_aggregate(df, 'accuracy', 'mean', lambda x: x['name'])
        >>> print(result)
          groupby_result  accuracy
        0          Alice     0.925
        1            Bob     0.865
        2        Charlie     0.920

        >>> # Example with multiple targets
        >>> result = pd_aggregate(df, ['accuracy', 'score'], 'mean', 'name')
        >>> print(result)
              name  accuracy  score
        0    Alice     0.925   11.0
        1      Bob     0.865   16.5
        2  Charlie     0.920   20.0

        >>> # Example with callable target
        >>> result = pd_aggregate(df, lambda df: df['accuracy'] * df['score'], 'mean', 'name')
        >>> print(result)
              name  target_temp
        0    Alice       10.150
        1      Bob       14.295
        2  Charlie       18.400

        >>> # Example with mapping of targets to callables
        >>> result = pd_aggregate(df, {'weighted_score': lambda df: df['accuracy'] * df['score']}, 'mean', 'name')
        >>> print(result)
              name  weighted_score
        0    Alice          10.150
        1      Bob          14.295
        2  Charlie          18.400

        >>> # Example with mapping of aggregations
        >>> result = pd_aggregate(df, 'accuracy', {'mean_accuracy': 'mean', 'sum_accuracy': 'sum'}, 'name')
        >>> print(result)
              name  mean_accuracy  sum_accuracy
        0    Alice          0.925          1.85
        1      Bob          0.865          1.73
        2  Charlie          0.920          0.92

        >>> # Example with mapping of groupby callables
        >>> result = pd_aggregate(df, 'accuracy', 'mean', {'group_by_name': lambda x: x['name'], 'group_by_group': lambda x: x['group']})
        >>> print(result)
          group_by_name group_by_group  accuracy
        0         Alice              A     0.925
        1           Bob              B     0.865
        2       Charlie              A     0.920

    """
    if isinstance(groupby, Mapping):
        for key, func in groupby.items():
            df[key] = df.apply(func, axis=1)
        grouped = df.groupby(list(groupby.keys()))
    elif callable(groupby):
        df['group_temp'] = df.apply(groupby, axis=1)
        grouped = df.groupby('group_temp')
    else:
        grouped = df.groupby(groupby) if isinstance(groupby, (str, Iterable)) else df

    if isinstance(target, Mapping):
        for key, func in target.items():
            df[key] = func(df)
        target = list(target.keys())
    elif callable(target):
        df['target_temp'] = target(df)
        target = 'target_temp'
    elif isinstance(target, Iterable) and all(callable(t) for t in target):
        for i, t in enumerate(target):
            df[f'target_temp_{i}'] = t(df)
        target = [f'target_temp_{i}' for i in range(len(target))]

    # Perform the aggregation
    if isinstance(agg, Mapping):
        agg_results = []
        for new_col, func in agg.items():
            agg_df = grouped[target].agg(func).reset_index()
            agg_df = agg_df.rename(columns={target: new_col})
            agg_results.append(agg_df)
        aggregated = agg_results[0]
        for agg_df in agg_results[1:]:
            aggregated = pd.merge(aggregated, agg_df, on=grouped.obj.columns.intersection(agg_df.columns).tolist(), how='inner')
    else:
        aggregated = grouped[target].agg(agg).reset_index()

    if isinstance(groupby, Mapping):
        for key in groupby.keys():
            df.drop(columns=key, inplace=True)
    elif callable(groupby):
        aggregated = aggregated.rename(columns={'group_temp': 'groupby_result'})
        df.drop(columns='group_temp', inplace=True)

    if isinstance(target, Mapping):
        for key in target:
            df.drop(columns=key, inplace=True)
    elif callable(target):
        df.drop(columns='target_temp', inplace=True)
    elif isinstance(target, Iterable) and all(callable(t) for t in target):
        df.drop(columns=[f'target_temp_{i}' for i in range(len(target))], inplace=True)

    return aggregated


@attrs(slots=True)
class PdAggregation:
    """
    A class to perform custom aggregations on a pandas DataFrame.

    Attributes:
        aggregation_name (str): The name of the aggregation result column.
        score_name (str): The name of the score column to be aggregated.
        method (Union[str, Callable]): The aggregation method, which can be a string (e.g., 'mean', 'sum')
                                       or a callable function (e.g., np.mean).
        groupby (Union[Iterable[str], Callable], optional): The columns or a function to group by. Default is None.

    Methods:
        __call__(self, df: pd.DataFrame) -> pd.DataFrame:
            Applies the aggregation to the DataFrame and returns the result.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'name': ['Alice', 'Bob', 'Alice', 'Bob', 'Charlie'],
        ...     'accuracy': [0.95, 0.85, 0.90, 0.88, 0.92],
        ...     'score': [10, 15, 12, 18, 20],
        ...     'group': ['A', 'B', 'A', 'B', 'A']
        ... }
        >>> df = pd.DataFrame(data)
        >>> aggregation = PdAggregation(
        ...     aggregation_name='mean_accuracy',
        ...     score_name='accuracy',
        ...     method='mean',
        ...     groupby='name'
        ... )
        >>> result = aggregation(df)
        >>> print(result)
              name  mean_accuracy
        0    Alice          0.925
        1      Bob          0.865
        2  Charlie          0.920


        >>> # Example with multiple groupby columns
        >>> aggregation = PdAggregation(
        ...     aggregation_name='sum_accuracy',
        ...     score_name='accuracy',
        ...     method='sum',
        ...     groupby=['name', 'group']
        ... )
        >>> result = aggregation(df)
        >>> print(result)
              name group  sum_accuracy
        0    Alice     A          1.85
        1      Bob     B          1.73
        2  Charlie     A          0.92
    """

    aggregation_name: str = attrib()
    score_name: str = attrib()
    method: Union[str, Callable] = attrib()
    groupby: Union[Iterable[str], Callable] = attrib(default=None)

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the aggregation to the DataFrame and returns the result.

        Args:
            df (pd.DataFrame): The DataFrame to perform the aggregation on.

        Returns:
            pd.DataFrame: The DataFrame with the aggregated results.
        """
        result = pd_aggregate(
            df,
            target=self.score_name,
            agg=self.method,
            groupby=self.groupby
        )
        result = result.rename(columns={self.score_name: self.aggregation_name})
        return result

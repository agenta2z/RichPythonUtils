import gzip
import pickle
import sys
from typing import Optional, Union

from rich_python_utils.path_utils.common import ensure_parent_dir_existence


def pickle_load(source: Union[str, bytes], compressed: bool = False, encoding=None):
    """
    Load a Python object from a pickle file or bytes.

    This function supports both plain and compressed (gzip) pickle files,
    as well as loading directly from bytes.
    If `source` is bytes, it loads directly from the bytes.
    If `source` is a string (file path) and `compressed=True`, the file is read 
    using gzip.open; otherwise, it's read using the standard open function.

    Args:
        source (Union[str, bytes]): The file path to the pickle file, or pickle bytes to load.
        compressed (bool, optional): If True, interpret the file/bytes as compressed with gzip. Defaults to False.
        encoding (str, optional): The encoding to use when loading the pickle file in Python 3.
            If None, default unencoded loading is used.
            On Python 2 or earlier (not typically relevant now), this parameter is ignored.

    Returns:
        Any: The Python object stored in the pickle file or bytes.

    Examples:
        >>> import tempfile, os
        >>> data = {'a': 1, 'b': 2}
        >>> tmp_file = os.path.join(tempfile.gettempdir(), "test_pickle.pkl")
        >>> with open(tmp_file, 'wb') as f:
        ...     pickle.dump(data, f)
        ...
        >>> loaded_data = pickle_load(tmp_file)
        >>> loaded_data == data
        True
        >>> os.remove(tmp_file)

        # Compressed example:
        >>> import gzip
        >>> tmp_compressed_file = os.path.join(tempfile.gettempdir(), "test_pickle_compressed.pkl.gz")
        >>> with gzip.open(tmp_compressed_file, 'wb') as f:
        ...     pickle.dump(data, f)
        ...
        >>> loaded_data_compressed = pickle_load(tmp_compressed_file, compressed=True)
        >>> loaded_data_compressed == data
        True
        >>> os.remove(tmp_compressed_file)

        # Load from bytes example:
        >>> data = {'x': 42}
        >>> pickle_bytes = pickle.dumps(data)
        >>> loaded_from_bytes = pickle_load(pickle_bytes)
        >>> loaded_from_bytes == data
        True
    """
    # If source is bytes, load directly from bytes
    if isinstance(source, bytes):
        if compressed:
            source = gzip.decompress(source)
        return pickle.loads(source)
    
    # Existing file load logic
    with open(source, 'rb') if not compressed else gzip.open(source, 'rb') as f:
        if encoding is None or sys.version_info < (3, 0):
            return pickle.load(f)

        else:
            return pickle.load(f, encoding=encoding)


def pickle_save(data, file_path: Optional[str] = None, compressed: bool = False, ensure_dir_exists=True, verbose: bool = __debug__) -> Optional[bytes]:
    """
    Save a Python object to a pickle file or return bytes.

    This function supports saving to plain or compressed (gzip) pickle files.
    If `file_path` is None, returns the pickled bytes instead of writing to a file.
    If `ensure_dir_exists=True`, it ensures that the parent directory of the file_path exists
    before saving the pickle. If `compressed=True`, it writes the pickle file with gzip compression.

    Args:
        data (Any): The Python object to save.
        file_path (Optional[str]): The path where the pickle file will be saved.
            If None, returns pickle bytes instead of writing to file.
        compressed (bool, optional): If True, save the file in gzip-compressed format. Defaults to False.
        ensure_dir_exists (bool, optional): If True, create the parent directory if it does not exist.
            Defaults to True.
        verbose (bool, optional): If True, print debug information when creating directories.
            Defaults to Python's __debug__ value.

    Returns:
        Optional[bytes]: None if file_path is provided (writes to file), 
            bytes if file_path is None (returns pickled data).

    Examples:
        >>> import tempfile, os
        >>> data = {'x': 42}
        >>> tmp_file = os.path.join(tempfile.gettempdir(), "test_pickle_output.pkl")
        >>> pickle_save(data, tmp_file, ensure_dir_exists=True, verbose=False)
        >>> os.path.exists(tmp_file)
        True
        >>> loaded_data = pickle_load(tmp_file)
        >>> loaded_data == data
        True
        >>> os.remove(tmp_file)

        # Compressed example:
        >>> tmp_compressed_file = os.path.join(tempfile.gettempdir(), "test_pickle_output_compressed.pkl.gz")
        >>> pickle_save(data, tmp_compressed_file, compressed=True, verbose=False)
        >>> os.path.exists(tmp_compressed_file)
        True
        >>> loaded_data_compressed = pickle_load(tmp_compressed_file, compressed=True)
        >>> loaded_data_compressed == data
        True
        >>> os.remove(tmp_compressed_file)

        # Return bytes example:
        >>> data = {'a': 1, 'b': 2}
        >>> pickle_bytes = pickle_save(data, None)
        >>> isinstance(pickle_bytes, bytes)
        True
        >>> pickle_load(pickle_bytes) == data
        True
    """
    # If file_path is None, return bytes directly
    if file_path is None:
        if compressed:
            return gzip.compress(pickle.dumps(data))
        return pickle.dumps(data)
    
    # Existing file save logic
    if ensure_dir_exists:
        ensure_parent_dir_existence(file_path, verbose=verbose)
    with open(file_path, 'wb+') if not compressed else gzip.open(file_path, 'wb+') as f:
        pickle.dump(data, f)
        # ! must flush and close to ensure data completeness
        # ! when this function is called in multi-processing or Spark
        f.flush()
        f.close()
    return None

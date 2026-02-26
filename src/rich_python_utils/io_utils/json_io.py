import copy
import json
import os
import shutil
import uuid
from datetime import datetime
from itertools import chain, islice
from os import path
from typing import Union, Iterable, Iterator, Dict, List, Mapping, Type, Callable, Sequence, Optional

from rich_python_utils.common_objects.partial import Partial
from rich_python_utils.common_utils import dict__, get_relevant_named_args
from rich_python_utils.common_utils.iter_helper import iter__
from rich_python_utils.common_utils.map_helper import (
    has_path, get_at_path, set_at_path, delete_at_path, parse_key_path,
    obj_walk_through,
)
from rich_python_utils.console_utils import eprint_message
from rich_python_utils.io_utils.common import DEFAULT_ENCODING, open_, _default_space_handler, read_text_or_file
from rich_python_utils.io_utils.text_io import _get_input_file_stream, write_all_lines, read_all_text
from rich_python_utils.path_utils.path_string_operations import get_main_name, get_ext_name
from rich_python_utils.path_utils.path_listing import get_files_by_pattern, get_sorted_files_from_all_sub_dirs
from rich_python_utils.path_utils.common import ensure_dir_existence, resolve_ext

DEFAULT_JSON_FILE_PATTERN = '*.json*'

try:
    from enum import StrEnum

    class JsonConverter(StrEnum):
        DICT = 'dict'                                # Use dict__(recursive=True) — current default
        DICT_NON_RECURSIVE = 'dict_non_recursive'    # Use dict__(recursive=False)
        STR = 'str'                                  # Use str()
        NONE = 'none'                                # No conversion, pass to json.dumps as-is

    class PartsReplacementMode(StrEnum):
        REMOVE = 'remove'
        TRUNCATE = 'truncate'
        ABSOLUTE_PATH = 'absolute_path'
        RELATIVE_PATH = 'relative_path'
        FILENAME_ONLY = 'filename_only'
        REFERENCE = 'reference'

    class SpaceExtMode(StrEnum):
        NONE = 'none'
        MOVE = 'move'
        ADD = 'add'

except ImportError:
    from enum import Enum

    class JsonConverter(str, Enum):
        DICT = 'dict'
        DICT_NON_RECURSIVE = 'dict_non_recursive'
        STR = 'str'
        NONE = 'none'

    class PartsReplacementMode(str, Enum):
        REMOVE = 'remove'
        TRUNCATE = 'truncate'
        ABSOLUTE_PATH = 'absolute_path'
        RELATIVE_PATH = 'relative_path'
        FILENAME_ONLY = 'filename_only'
        REFERENCE = 'reference'

    class SpaceExtMode(str, Enum):
        NONE = 'none'
        MOVE = 'move'
        ADD = 'add'


class PartsKeyPath:
    """Describes a key path to extract as a separate parts file.

    Attributes:
        key: Dotted key path in the source dict (e.g. 'body_html' or 'response.content').
        ext: File extension override (e.g. '.html'). None to auto-detect.
        alias: Name alias used in the output filename instead of the key path.
        subfolder: Per-entry subdirectory between parts_subfolder and file_stem.
    """
    __slots__ = ('key', 'ext', 'alias', 'subfolder')

    def __init__(self, key: str, ext: str = None, alias: str = None, subfolder: str = None):
        self.key = key
        self.ext = ext
        self.alias = alias
        self.subfolder = subfolder

    def __repr__(self):
        fields = [repr(self.key)]
        if self.ext is not None:
            fields.append(f'ext={self.ext!r}')
        if self.alias is not None:
            fields.append(f'alias={self.alias!r}')
        if self.subfolder is not None:
            fields.append(f'subfolder={self.subfolder!r}')
        return f'PartsKeyPath({", ".join(fields)})'


def artifact_field(
    key: str,
    *,
    type: Optional[str] = None,
    alias: Optional[str] = None,
    group: Optional[str] = None,
) -> Callable[[Type], Type]:
    """Class decorator that marks a field as an artifact for parts extraction.

    Stack multiple decorators to mark multiple fields.  Apply **outside**
    ``@attrs`` so the metadata survives slot class creation::

        @artifact_field('body_html', type='html', group='ui_source')
        @artifact_field('cleaned_html', type='html', group='ui_source')
        @attrs(slots=True)
        class MyResult:
            body_html: str = attrib()
            cleaned_html: str = attrib()

    Args:
        key: Field name (or dotted key path) to extract.
        type: Content type (e.g. ``'html'``, ``'.html'``).  Mapped to
            :attr:`PartsKeyPath.ext` with automatic dot normalization.
        alias: Filename alias (replaces key in the output filename).
        group: Per-entry subdirectory.  Mapped to
            :attr:`PartsKeyPath.subfolder`.
    """

    def decorator(cls):
        if '__artifacts__' not in cls.__dict__:
            cls.__artifacts__ = []
        cls.__artifacts__.append(
            PartsKeyPath(key=key, ext=resolve_ext(type), alias=alias, subfolder=group)
        )
        return cls

    return decorator


def get_key_paths_for_artifacts(
    *classes: Type,
    groups: Optional[Union[str, Sequence[str]]] = None,
    recursive: bool = False,
) -> List[PartsKeyPath]:
    """Build a :class:`PartsKeyPath` list from ``@artifact_field`` metadata.

    Reads ``__artifacts__`` from each class and returns a flat list of
    :class:`PartsKeyPath` entries suitable for ``parts_key_paths``.

    Args:
        *classes: One or more classes decorated with :func:`artifact_field`.
        groups: Optional group name or sequence of group names to include.
            When ``None`` (default), all artifact fields are returned.
            When specified, only entries whose ``subfolder`` matches
            one of the given groups are included.
        recursive: If ``True``, use :func:`obj_walk_through` to walk the
            class annotations and discover nested ``@artifact_field``-decorated
            classes.  Fields already marked as artifacts on the parent are
            **not** recursed into (their children are skipped).

    Returns:
        List[PartsKeyPath]: Combined artifact entries from all classes.

    Example::

        logger = JsonLogger(
            file_path='logs/session.jsonl',
            parts_key_paths=get_key_paths_for_artifacts(WebDriverActionResult),
        )

        # Only 'ui_source' group:
        get_key_paths_for_artifacts(WebDriverActionResult, groups=['ui_source'])

        # Recursive discovery of nested artifact classes:
        get_key_paths_for_artifacts(OuterResult, recursive=True)
    """
    result = []
    if recursive:
        for cls in classes:
            _collect_artifacts_recursive(cls, result)
    else:
        for cls in classes:
            result.extend(cls.__dict__.get('__artifacts__', []))
    if groups is not None:
        if isinstance(groups, str):
            groups = (groups,)
        allowed = set(groups)
        result = [e for e in result if e.subfolder in allowed]
    return result


def _collect_artifacts_recursive(cls: Type, result: List[PartsKeyPath]) -> None:
    """Collect artifact entries from *cls* and nested types via walk-through.

    Uses :func:`obj_walk_through` in type mode to discover nested classes
    with ``__artifacts__``.  If a field is itself marked as an artifact on its
    parent, its children are not recursed into (the parent artifact already
    covers that subtree).
    """
    # Direct artifacts from the root class
    direct = cls.__dict__.get('__artifacts__', [])
    result.extend(direct)

    # Artifact keys discovered so far — used by should_recurse to prune
    # subtrees whose root field is already an artifact.
    artifact_keys = {(e.key,) for e in direct}

    def _should_recurse(path, _child):
        return tuple(path) not in artifact_keys

    for path, field_type in obj_walk_through(cls, should_recurse=_should_recurse):
        path_tuple = tuple(path)
        # Skip nodes that are themselves artifacts on their parent
        if path_tuple in artifact_keys:
            continue
        if isinstance(field_type, type) and hasattr(field_type, '__artifacts__'):
            prefix = '.'.join(path)
            for entry in field_type.__artifacts__:
                result.append(PartsKeyPath(
                    key=f'{prefix}.{entry.key}',
                    ext=entry.ext,
                    alias=entry.alias,
                    subfolder=entry.subfolder,
                ))
                # Mark child artifact so its subtree is pruned too
                artifact_keys.add(path_tuple + (entry.key,))


class JsonLogger(Partial):
    """Callable wrapper around :func:`write_json` for use as a Debuggable logger.

    Replaces ``functools.partial(write_json, file_path=..., append=True)`` with
    parameter-mapping support.  Currently maps:

        group               →  subfolder
        max_message_length  →  leaf_as_parts_if_exceeding_size

    Attributes:
        file_path: Exposed for external discovery of the log path.

    Examples:
        >>> logger = JsonLogger(file_path='logs/session.jsonl', append=True)
        >>> logger                                            # doctest: +ELLIPSIS
        JsonLogger(file_path='logs/session.jsonl', append=True)

        Direct replacement for ``partial(write_json, ...)``:

        >>> from rich_python_utils.common_objects.debuggable import Debuggable
        >>> class MyObj(Debuggable): pass
        >>> obj = MyObj(logger=JsonLogger(file_path='logs/s.jsonl', append=True),
        ...            debug_mode=True, always_add_logging_based_logger=False)
        >>> obj.logger                                        # doctest: +ELLIPSIS
        {...: JsonLogger(file_path='logs/s.jsonl', append=True)}
    """

    _PARAM_MAP = {
        'group': 'subfolder',
        'max_message_length': 'leaf_as_parts_if_exceeding_size',
    }

    _FIRST_ARG_VALUES_TO_PARAM_MAP = {
        'type': 'parts_subfolder'
    }

    def __init__(self, **kwargs):
        super().__init__(write_json, **kwargs)

    @property
    def file_path(self):
        return self._kwargs.get('file_path')

    def __repr__(self):
        params = ', '.join(f'{k}={v!r}' for k, v in self._kwargs.items())
        return f'JsonLogger({params})' if params else 'JsonLogger()'


class JsonLogReader:
    """Iterable reader for JSON log files, counterpart to :class:`JsonLogger`.

    Wraps :func:`iter_json_objs` with baked-in parameters for convenient
    repeated reading.  Defaults differ from ``iter_json_objs`` to suit
    log-reading workflows: ``resolve_parts=True``, ``use_tqdm=False``,
    ``verbose=False``.

    Each call to ``iter()`` / ``for obj in reader`` creates a fresh iterator
    over the file.

    Examples:
        >>> reader = JsonLogReader(file_path='logs/session.jsonl')
        >>> for obj in reader:
        ...     print(obj.get('type'))
    """

    def __init__(
        self,
        file_path: str,
        resolve_parts: bool = True,
        parts_suffix: str = '.parts',
        selection=None,
        result_type=dict,
        encoding: str = None,
        ignore_error: bool = False,
        top: int = None,
        verbose: bool = False,
        use_tqdm: bool = False,
        json_file_pattern: str = None,
    ):
        self._file_path = file_path
        self._kwargs = dict(
            resolve_parts=resolve_parts,
            parts_suffix=parts_suffix,
            selection=selection,
            result_type=result_type,
            encoding=encoding,
            ignore_error=ignore_error,
            top=top,
            verbose=verbose,
            use_tqdm=use_tqdm,
        )
        if json_file_pattern is not None:
            self._kwargs['json_file_pattern'] = json_file_pattern

    @property
    def file_path(self):
        """The log file path, mirroring :attr:`JsonLogger.file_path`."""
        return self._file_path

    def __iter__(self):
        return iter_json_objs(self._file_path, **self._kwargs)

    def __repr__(self):
        params = ', '.join(f'{k}={v!r}' for k, v in self._kwargs.items())
        fp = f'file_path={self._file_path!r}'
        inner = f'{fp}, {params}' if params else fp
        return f'JsonLogReader({inner})'


def _normalize_extract_path_entry(entry):
    """
    Normalizes an extract path entry into (path_str, ext_override, name_alias, entry_subfolder).

    Accepts:
        - 'item'                                          → ('item', None, None, None)
        - ('body_html', '.html')                          → ('body_html', '.html', None, None)
        - ('body_html', '.html', 'BodyHtml')              → ('body_html', '.html', 'BodyHtml', None)
        - ('body_html', '.html', None, 'ui_source/html')  → ('body_html', '.html', None, 'ui_source/html')
        - PartsKeyPath('body_html', ext='.html')          → ('body_html', '.html', None, None)
    """
    if isinstance(entry, PartsKeyPath):
        return entry.key, entry.ext, entry.alias, entry.subfolder
    if isinstance(entry, (list, tuple)):
        path_str = str(entry[0])
        ext_override = entry[1] if len(entry) > 1 else None
        name_alias = entry[2] if len(entry) > 2 else None
        entry_subfolder = str(entry[3]) if len(entry) > 3 and entry[3] is not None else None
        return path_str, ext_override, name_alias, entry_subfolder
    return str(entry), None, None, None


def _detect_extension(value):
    """Auto-detect file extension based on value content."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:5].lower() == '<html' or stripped[:5].lower() == '<!doc':
            return '.html'
        return '.txt'
    return '.json'


def _serialize_value(value, ensure_ascii=False):
    """Serialize a value to string for writing to a parts file."""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=ensure_ascii, indent=2)


def _resolve_parts_references(obj, parts_dir):
    """
    Recursively walk a dict tree and replace __parts_file__ reference markers
    with the loaded file content.

    Only works with REFERENCE replacement mode markers:
        {"__parts_file__": "filename.txt", "__value_type__": "str"|"json"}
    """
    if isinstance(obj, dict):
        if (
            len(obj) == 2
            and '__parts_file__' in obj
            and '__value_type__' in obj
        ):
            parts_file = path.join(parts_dir, obj['__parts_file__'])
            if path.isfile(parts_file):
                with open(parts_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                if obj['__value_type__'] == 'json':
                    return json.loads(content)
                return content
            return obj
        return {k: _resolve_parts_references(v, parts_dir) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_resolve_parts_references(item, parts_dir) for item in obj]
    return obj


def resolve_json_parts(
        obj,
        source_path: str,
        parts_suffix: str = '.parts',
):
    """
    Resolve ``__parts_file__`` reference markers in a JSON object by loading
    the referenced file content from the adjacent parts directory.

    This is the read-side counterpart to ``jsonfy()`` with parts extraction.
    Only works with REFERENCE replacement mode markers:
        {"__parts_file__": "filename.txt", "__value_type__": "str"|"json"}

    Args:
        obj: A JSON-serializable object (typically a dict) potentially containing
            parts reference markers.
        source_path (str): Path to the source JSON file. The parts directory is located
            at ``source_path + parts_suffix``.
        parts_suffix (str): Suffix used to locate the parts directory. Default is '.parts'.

    Returns:
        The object with all parts references resolved (file contents loaded in-place).
        If the parts directory does not exist, returns obj unchanged.
    """
    if isinstance(obj, dict) and isinstance(source_path, str):
        parts_dir = source_path + parts_suffix
        if path.isdir(parts_dir):
            return _resolve_parts_references(obj, parts_dir)
    return obj


def iter_all_json_strs(json_obj_iter, process_func=None, indent=None, ensure_ascii=False, **kwargs):
    if process_func:
        for json_obj in json_obj_iter:
            try:
                yield json.dumps(process_func(json_obj), indent=indent, ensure_ascii=ensure_ascii, **kwargs)
            except Exception as ex:
                print(json_obj)
                raise ex
    else:
        for json_obj in json_obj_iter:
            try:
                yield json.dumps(json_obj, indent=indent, ensure_ascii=ensure_ascii, **kwargs)
            except Exception as ex:
                print(json_obj)
                raise ex


def _iter_json_objs(
        json_input: Union[str, Iterable, Iterator],
        use_tqdm: bool = True,
        disp_msg: str = None,
        verbose: bool = __debug__,
        encoding: str = DEFAULT_ENCODING,
        ignore_error: bool = False,
        top: int = None,
        selection: Union[str, Iterable[str]] = None,
        result_type: Union[str, Type] = dict,
        json_file_pattern: str = DEFAULT_JSON_FILE_PATTERN,
        resolve_parts: bool = False,
        parts_suffix: str = '.parts',
) -> Iterator[Dict]:
    """
    Iterates over JSON objects from various input sources.

    Args:
        json_input (Union[str, Iterable, Iterator]): The JSON input source, which can be a file path, directory path, or an iterable/iterator of JSON strings.
        use_tqdm (bool): Whether to display a progress bar using tqdm. Default is True.
        disp_msg (str): A message to display with the progress bar. Default is None.
        verbose (bool): Whether to print verbose messages. Default is __debug__.
        encoding (str): The encoding of the input file. Default is None.
        ignore_error (bool): Whether to ignore errors during JSON parsing. Default is False.
        top (int): The maximum number of JSON objects to read. Default is None.
        selection (Union[str, Iterable[str]]): A subset of JSON keys to include in the output. Default is None.
        result_type (Union[str, Type]): The type of the result, either dict, list, or tuple. Default is dict.
        json_file_pattern (str): File pattern to use when searching for JSON files in a directory. Default is DEFAULT_JSON_FILE_PATTERN ('*.json*').
        resolve_parts (bool): If True, resolve '__parts_file__' reference markers in each
            JSON object by loading the referenced file content from the adjacent parts
            directory. Only works with REFERENCE replacement mode markers. Default is False.
        parts_suffix (str): Suffix used to locate the parts directory relative to each
            source file (e.g., 'data.json' -> 'data.json.parts'). Default is '.parts'.

    Yields:
        Dict: The next JSON object parsed from the input source.

    Raises:
        ValueError: If `result_type` is not one of dict, list, or tuple.
        Exception: If an error occurs during JSON parsing and `ignore_error` is not True or 'silent'.

    Examples:
        >>> import tempfile
        >>> with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
        ...     tmp_file.write('{"key1": "value1", "key2": "value2"}\\n{"key1": "value3", "key2": "value4"}')
        ...     tmp_file.flush()
        ...     tmp_file_path = tmp_file.name
        ...     print(list(_iter_json_objs(tmp_file_path)))
        ...     print(list(_iter_json_objs(tmp_file_path, selection=['key1'])))
        73
        [{'key1': 'value1', 'key2': 'value2'}, {'key1': 'value3', 'key2': 'value4'}]
        [{'key1': 'value1'}, {'key1': 'value3'}]

        >>> # Clean up the temporary file
        >>> import os
        >>> os.remove(tmp_file_path)

    """

    def _process_json_obj(json_obj):
        if selection:
            if result_type is dict or result_type == 'dict':
                json_obj = {
                    k: json_obj[k] for k in iter__(selection)
                }
            elif (
                    (result_type is list)
                    or (result_type is tuple)
                    or result_type == 'list'
                    or result_type == 'tuple'
            ):
                json_obj = result_type(json_obj[k] for k in iter__(selection))
        elif (
                (result_type is list)
                or (result_type is tuple)
                or result_type == 'list'
                or result_type == 'tuple'
        ):
            json_obj = result_type(json_obj.values())
        return json_obj

    def _maybe_resolve_parts(json_obj, source_file):
        if resolve_parts:
            return resolve_json_parts(json_obj, source_file, parts_suffix=parts_suffix)
        return json_obj

    def _iter_single_input(json_input):

        if not (
                (result_type is dict)
                or (result_type is list)
                or (result_type is tuple)
                or result_type in ('dict', 'list', 'tuple')
        ):
            raise ValueError("'result_type' must be one of dict, list or tuple")

        # get an iterator for the json input
        line_iter, fin = _get_input_file_stream(
            file=json_input,
            encoding=encoding,
            top=top,
            use_tqdm=use_tqdm,
            display_msg=disp_msg or 'read json object from {}',
            verbose=verbose
        )
        # iterate through the json input
        try_parse_as_single_line = False
        _source_file = json_input if isinstance(json_input, str) else None
        for line in line_iter:
            if line:
                try:
                    try:
                        json_obj = json.loads(line)
                    except json.decoder.JSONDecodeError:
                        try_parse_as_single_line = True
                        break

                    yield _process_json_obj(_maybe_resolve_parts(json_obj, _source_file))
                except Exception as ex:
                    if ignore_error is True:
                        print(line)
                        print(ex)
                    elif ignore_error == 'silent':
                        continue
                    else:
                        print(line)
                        raise ex

        if try_parse_as_single_line:
            try:
                json_obj = read_single_line_json_file(json_input)
                if isinstance(json_obj, Dict):
                    yield _process_json_obj(_maybe_resolve_parts(json_obj, _source_file))
                else:
                    json_objs = json_obj
                    for json_obj in json_objs:
                        yield _process_json_obj(_maybe_resolve_parts(json_obj, _source_file))
            except:
                pass

        fin.close()

    if isinstance(json_input, str):
        if path.isfile(json_input):
            # If input is a file, iterate over JSON objects in the file
            yield from _iter_single_input(json_input)
        else:
            # assuming otherwise the input is a directory,
            # then iterate over JSON objects in the json files under the directory
            for _json_input in get_files_by_pattern(
                    json_input,
                    pattern=json_file_pattern,
                    full_path=True,
                    recursive=False,
                    sort=True
            ):
                try:
                    yield from _iter_single_input(_json_input)
                except Exception as ex:
                    if verbose:
                        eprint_message(
                            'reading JSON file failed', json_input
                        )
                    raise ex
    else:
        # otherwise, just try iterating the input,
        # assuming the input is itself an iterator or iterable
        try:
            yield from _iter_single_input(json_input)
        except Exception as ex:
            if verbose:
                eprint_message(
                    'reading JSON file failed', json_input
                )
            raise ex


def read_single_line_json_file(json_input: Union[str, Iterable, Iterator]):
    """
    Reads JSON objects from a file where each line contains a JSON object.

    Args:
        json_input: A path to a file, an iterable of file paths, or an iterator yielding lines of JSON.

    Returns:
        A list of JSON objects read from the input file(s).

    Examples:
        # Read a JSON object from a single file:
        >>> import tempfile
        >>> import os
        >>> tmp_file = tempfile.NamedTemporaryFile(delete=False)
        >>> tmp_name = tmp_file.name
        >>> tmp_file.close()
        >>> write_json({'x': 1, 'y': 2}, tmp_name)
        overwrite file ...
        >>> data = read_single_line_json_file(tmp_name)
        >>> print(data)
        {'x': 1, 'y': 2}
        >>> os.unlink(tmp_name)


        # Read JSON objects from multiple files:
        >>> tmp_file1 = tempfile.NamedTemporaryFile(delete=False)
        >>> tmp_file2 = tempfile.NamedTemporaryFile(delete=False)
        >>> tmp_name1, tmp_name2 = tmp_file1.name, tmp_file2.name
        >>> tmp_file1.close()
        >>> tmp_file2.close()
        >>> write_json({'a': 1}, tmp_name1)
        overwrite file ...
        >>> write_json({'b': 2}, tmp_name2)
        overwrite file ...
        >>> data = read_single_line_json_file([tmp_name1, tmp_name2])
        >>> print(sorted(data, key=lambda x: list(x.keys())[0]))
        [{'a': 1}, {'b': 2}]
        >>> os.unlink(tmp_name1)
        >>> os.unlink(tmp_name2)
    """
    if isinstance(json_input, str):
        return json.loads(read_all_text(json_input))
    else:
        out = []
        for _input_path in json_input:
            loaded_jobj_or_jobj_list = json.loads(read_all_text(_input_path))
            if isinstance(loaded_jobj_or_jobj_list, Mapping):
                out.append(loaded_jobj_or_jobj_list)
            else:
                out.extend(loaded_jobj_or_jobj_list)

        return out


def read_json(json_text_or_file: str):
    return read_text_or_file(
        text_or_file=json_text_or_file,
        read_text_func=json.loads,
        read_file_func=read_single_line_json_file
    )


def read_jsonl(jsonl_text_or_file: str):
    return read_text_or_file(
        text_or_file=jsonl_text_or_file,
        read_text_func=lambda _: [json.loads(json_line) for json_line in jsonl_text_or_file.split('\n') if json_line],
        read_file_func=lambda _: list(iter_json_objs(jsonl_text_or_file))
    )


def iter_json_objs(
        json_input: Union[str, Iterable, Iterator],
        use_tqdm: bool = True,
        disp_msg: str = None,
        verbose: bool = __debug__,
        encoding: str = DEFAULT_ENCODING,
        ignore_error: bool = False,
        top: int = None,
        selection: Union[str, Iterable[str]] = None,
        result_type: Union[str, Type] = dict,
        return_input_source_and_index_for_each_json_obj: bool = False,
        json_file_pattern: str = DEFAULT_JSON_FILE_PATTERN,
        resolve_parts: bool = False,
        parts_suffix: str = '.parts',
) -> Iterator[Dict]:
    """
    Iterate through all JSON objects in a file, all JSON objects in all '.json' files in a directory,
    or a text line iterator.

    Args:
        json_input (Union[str, Iterable, Iterator]): Path to a JSON file, a directory containing JSON files,
            or an iterable/iterator of JSON strings.
        use_tqdm (bool): If True, use tqdm to display reading progress. Default is True.
        disp_msg (str): Message to display with the progress bar. Default is None.
        verbose (bool): If True, print the `disp_msg` regardless of `use_tqdm`. Default is __debug__.
        encoding (str): File encoding to use when reading from a file, such as 'utf-8'. Default is None.
        ignore_error (bool): If True, ignore errors when parsing JSON objects. Default is False.
        top (int): Maximum number of JSON objects to read. Default is None (read all).
        selection (Union[str, Iterable[str]]): One or more keys to select fields from the returned JSON objects. Default is None.
        result_type (Union[str, Type]): The type of the result, either dict, list, or tuple. Default is dict.
        return_input_source_and_index_for_each_json_obj (bool): If True, return the input source and index along with each JSON object. Default is False.
        json_file_pattern (str): File pattern to use when searching for JSON files in a directory. Default is DEFAULT_JSON_FILE_PATTERN ('*.json*').
        resolve_parts (bool): If True, resolve '__parts_file__' reference markers in each
            JSON object by loading the referenced file content from the adjacent parts
            directory. Only works with REFERENCE replacement mode markers. Default is False.
        parts_suffix (str): Suffix used to locate the parts directory relative to each
            source file (e.g., 'data.json' -> 'data.json.parts'). Default is '.parts'.

    Yields:
        Iterator[Dict]: The next JSON object parsed from the input source. If `return_input_for_each_json_obj`
        is True, yields a tuple of the JSON object and the input source.

    Raises:
        ValueError: If `result_type` is not one of dict, list, or tuple.
        Exception: If an error occurs during JSON parsing and `ignore_error` is not True or 'silent'.

    Examples:
        >>> import tempfile
        >>> tmp_file_path1 = './tmp1'
        >>> tmp_file_path2 = './tmp2'
        >>> with open(tmp_file_path1, 'w+') as tmp_file:
        ...     tmp_file.write('{"key1": "value1", "key2": "value2"}\\n{"key1": "value3", "key2": "value4"}')
        ...     tmp_file.flush()
        ...     print(list(iter_json_objs(tmp_file_path1)))
        ...     print(list(iter_json_objs(tmp_file_path1, selection=['key1'])))
        ...     with open(tmp_file_path2, 'w+') as tmp_file2:
        ...         tmp_file2.write('{"key1": "value5", "key2": "value6"}\\n{"key1": "value7", "key2": "value8"}')
        ...         tmp_file2.flush()
        ...         print(list(iter_json_objs([tmp_file_path1, tmp_file_path2], return_input_source_and_index_for_each_json_obj=True)))
        ...         print(list(iter_json_objs([tmp_file_path1, tmp_file_path2], selection=['key1'], return_input_source_and_index_for_each_json_obj=True)))
        73
        [{'key1': 'value1', 'key2': 'value2'}, {'key1': 'value3', 'key2': 'value4'}]
        [{'key1': 'value1'}, {'key1': 'value3'}]
        73
        [({'key1': 'value1', 'key2': 'value2'}, './tmp1', 0), ({'key1': 'value3', 'key2': 'value4'}, './tmp1', 1), ({'key1': 'value5', 'key2': 'value6'}, './tmp2', 0), ({'key1': 'value7', 'key2': 'value8'}, './tmp2', 1)]
        [({'key1': 'value1'}, './tmp1', 0), ({'key1': 'value3'}, './tmp1', 1), ({'key1': 'value5'}, './tmp2', 0), ({'key1': 'value7'}, './tmp2', 1)]


        >>> # Clean up the temporary file
        >>> import os
        >>> os.remove(tmp_file_path1)
        >>> os.remove(tmp_file_path2)
    """

    if isinstance(json_input, (tuple, list, set)):
        if return_input_source_and_index_for_each_json_obj:
            for _json_input in json_input:
                for jobj_index, jobj in enumerate(
                        _iter_json_objs(
                            json_input=_json_input,
                            use_tqdm=use_tqdm,
                            disp_msg=disp_msg,
                            verbose=verbose,
                            encoding=encoding,
                            ignore_error=ignore_error,
                            top=top,
                            selection=selection,
                            result_type=result_type,
                            json_file_pattern=json_file_pattern,
                        resolve_parts=resolve_parts,
                        parts_suffix=parts_suffix,
                        )
                ):
                    yield jobj, _json_input, jobj_index
        else:
            for _json_input in json_input:
                yield from _iter_json_objs(
                    json_input=_json_input,
                    use_tqdm=use_tqdm,
                    disp_msg=disp_msg,
                    verbose=verbose,
                    encoding=encoding,
                    ignore_error=ignore_error,
                    top=top,
                    selection=selection,
                    result_type=result_type,
                    json_file_pattern=json_file_pattern,
                    resolve_parts=resolve_parts,
                    parts_suffix=parts_suffix,
                )
    else:
        if return_input_source_and_index_for_each_json_obj:
            for jobj_index, jobj in enumerate(
                    _iter_json_objs(
                        json_input=json_input,
                        use_tqdm=use_tqdm,
                        disp_msg=disp_msg,
                        verbose=verbose,
                        encoding=encoding,
                        ignore_error=ignore_error,
                        top=top,
                        selection=selection,
                        result_type=result_type,
                        json_file_pattern=json_file_pattern,
                        resolve_parts=resolve_parts,
                        parts_suffix=parts_suffix,
                    )
            ):
                yield jobj, json_input, jobj_index
        else:
            yield from _iter_json_objs(
                json_input=json_input,
                use_tqdm=use_tqdm,
                disp_msg=disp_msg,
                verbose=verbose,
                encoding=encoding,
                ignore_error=ignore_error,
                top=top,
                selection=selection,
                result_type=result_type,
                json_file_pattern=json_file_pattern,
                resolve_parts=resolve_parts,
                parts_suffix=parts_suffix,
            )


def _iter_all_json_objs_from_all_sub_dirs(
        input_path: str,
        pattern: str = DEFAULT_JSON_FILE_PATTERN,
        use_tqdm: bool = False,
        display_msg: str = None,
        verbose: bool = __debug__,
        encoding: str = DEFAULT_ENCODING,
        ignore_error: bool = False,
        top: int = None,
        selection: Union[str, Iterable[str]] = None,
        result_type: Union[str, Type] = dict,
        return_input_source_and_index_for_each_json_obj: bool = False,
        resolve_parts: bool = False,
        parts_suffix: str = '.parts',
) -> Iterator[Dict]:
    """
    Iterate through all JSON objects from all subdirectories (including nested subdirectories)
    of a given directory, matching a specified pattern.

    Args:
        input_path: Path to the parent directory containing the subdirectories or path to the JSON file.
        pattern: Search for files of this pattern, e.g., '*.json'. Default is '*.json'.
        use_tqdm: If True, use tqdm to display reading progress. Default is False.
        display_msg: Message to display for this reading.
        verbose: If True, print out the display_msg regardless of use_tqdm. Default is __debug__.
        encoding: File encoding, such as 'utf-8'.
        ignore_error: If True, ignore JSON decoding errors and continue. Default is False.
        top: Number of lines to read from each file.
        selection: one or more keys to select fields from the returned json objs.
        result_type: can be one of dict, list or tuple; if 'list' or 'tuple' is specified,
            then only values are returned as a list or a tuple.
        resolve_parts (bool): If True, resolve '__parts_file__' reference markers in each
            JSON object by loading the referenced file content from the adjacent parts
            directory. Only works with REFERENCE replacement mode markers. Default is False.
        parts_suffix (str): Suffix used to locate the parts directory relative to each
            source file (e.g., 'data.json' -> 'data.json.parts'). Default is '.parts'.
    Returns:
        An iterator yielding JSON objects found in all subdirectories of the given parent directory.

    """
    if path.isfile(input_path):
        all_files = [input_path]
    else:
        all_files = get_sorted_files_from_all_sub_dirs(dir_path=input_path, pattern=pattern)

    yield from iter_json_objs(
        json_input=all_files,
        use_tqdm=use_tqdm,
        disp_msg=display_msg,
        verbose=verbose,
        encoding=encoding,
        ignore_error=ignore_error,
        top=top,
        selection=selection,
        result_type=result_type,
        return_input_source_and_index_for_each_json_obj=return_input_source_and_index_for_each_json_obj,
        resolve_parts=resolve_parts,
        parts_suffix=parts_suffix,
    )


def iter_all_json_objs_from_all_sub_dirs(
        input_path_or_paths: Union[str, Iterable[str]],
        pattern: str = DEFAULT_JSON_FILE_PATTERN,
        use_tqdm: bool = False,
        display_msg: str = None,
        verbose: bool = __debug__,
        encoding: str = DEFAULT_ENCODING,
        ignore_error: bool = False,
        top: int = None,
        top_per_input_path: int = None,
        selection: Union[str, Iterable[str]] = None,
        result_type: Union[str, Type] = dict,
        return_input_source_and_index_for_each_json_obj: bool = False,
        resolve_parts: bool = False,
        parts_suffix: str = '.parts',
) -> Iterator[Dict]:
    """
    Iterate through all JSON objects from all subdirectories of a given directory or directories,
    matching a specified pattern.

    Args:
        input_path_or_paths: Path to the parent directory containing the subdirectories or path to the
                             JSON file(s), or a list of such paths.
        pattern: Search for files of this pattern, e.g., '*.json'. Default is '*.json'.
        use_tqdm: If True, use tqdm to display reading progress. Default is False.
        display_msg: Message to display for this reading.
        verbose: If True, print out the display_msg regardless of use_tqdm. Default is __debug__.
        encoding: File encoding. Default is None.
        ignore_error: If True, ignore JSON decoding errors and continue. Default is False.
        top: Total number of JSON objects to read from all input files.
        top_per_input_path: Number of JSON objects to read from each input path;
            not effective if there is only one input path.
        selection: one or more keys to select fields from the returned json objs.
        result_type: can be one of dict, list or tuple; if 'list' or 'tuple' is specified,
            then only values are returned as a list or a tuple.
        resolve_parts (bool): If True, resolve '__parts_file__' reference markers in each
            JSON object by loading the referenced file content from the adjacent parts
            directory. Only works with REFERENCE replacement mode markers. Default is False.
        parts_suffix (str): Suffix used to locate the parts directory relative to each
            source file (e.g., 'data.json' -> 'data.json.parts'). Default is '.parts'.

    Returns:
        An iterator yielding JSON objects found in all subdirectories of the given parent directory
        or directories.

    """

    if isinstance(input_path_or_paths, str):
        return _iter_all_json_objs_from_all_sub_dirs(
            input_path=input_path_or_paths,
            pattern=pattern,
            use_tqdm=use_tqdm,
            display_msg=display_msg,
            verbose=verbose,
            encoding=encoding,
            ignore_error=ignore_error,
            top=top,
            selection=selection,
            result_type=result_type,
            return_input_source_and_index_for_each_json_obj=return_input_source_and_index_for_each_json_obj,
            resolve_parts=resolve_parts,
            parts_suffix=parts_suffix,
        )
    else:
        _it = chain(
            *(
                _iter_all_json_objs_from_all_sub_dirs(
                    input_path=input_path,
                    pattern=pattern,
                    use_tqdm=use_tqdm,
                    display_msg=display_msg,
                    verbose=verbose,
                    encoding=encoding,
                    ignore_error=ignore_error,
                    top=top_per_input_path,
                    selection=selection,
                    result_type=result_type,
                    return_input_source_and_index_for_each_json_obj=return_input_source_and_index_for_each_json_obj,
                    resolve_parts=resolve_parts,
                    parts_suffix=parts_suffix,
                )
                for input_path in input_path_or_paths
            )
        )
        if top:
            _it = islice(_it, top)
        return _it


def write_json_objs(
        json_obj_iter,
        output_path,
        process_func=None,
        use_tqdm=False,
        disp_msg=None,
        append=False,
        encoding='utf-8',
        ensure_ascii=False,
        indent=None,
        chunk_size: int = None,
        chunk_name_format: str = 'part_{:05}',
        chunked_file_ext_name: str = '.json',
        verbose=__debug__,
        create_dir=True,
        pid=None,
        **kwargs
):
    if pid is not None:
        output_path = path.join(
            path.dirname(output_path),
            get_main_name(output_path),
            f'{pid}.{get_ext_name(output_path)}'
        )

    write_all_lines(
        iterable=iter_all_json_strs(json_obj_iter, process_func, indent=indent, ensure_ascii=ensure_ascii, **kwargs),
        output_path=output_path,
        use_tqdm=use_tqdm,
        display_msg=disp_msg,
        append=append,
        encoding=encoding,
        verbose=verbose,
        create_dir=create_dir,
        chunk_size=chunk_size,
        chunk_name_format=chunk_name_format,
        chunked_file_ext_name=chunked_file_ext_name
    )


def jsonfy(
        obj,
        recursively_ensure_json_conversion: bool = True,
        converter: Union[JsonConverter, str, Callable, None] = None,
        parts_root_path: str = None,
        parts_key_paths=None,
        parts_min_size: int = 0,
        parts_mode: Union[PartsReplacementMode, str] = 'reference',
        parts_suffix: str = '.parts',
        parts_preview_len: int = 200,
        parts_preview_with_path: bool = False,
        parts_path_as_url: bool = False,
        parts_file_namer: Callable = None,
        parts_subfolder: str = None,
        parts_group_by_key: bool = False,
        parts_key_path_root: str = None,
        artifacts_as_parts=None,
        leaf_as_parts_if_exceeding_size: int = None,
        is_artifact=False,
        overwrite: bool = True,
        ensure_ascii: bool = False,
):
    """
    Converts an object to a JSON-serializable form, optionally extracting
    large values into separate .parts/ files.

    This function performs two logical steps:

    1. **Conversion**: Converts the object to a dict/JSON-serializable form
       using the specified ``converter`` or the default ``dict__`` conversion.
    2. **Parts extraction** (optional): If ``parts_key_paths`` is set and
       ``parts_root_path`` is provided, extracts designated values into separate
       files under a ``.parts/`` directory adjacent to the target file.

    When ``parts_root_path`` is None, only conversion is performed — no files
    are written and parts extraction is skipped.

    Args:
        obj: The object to convert. Can be any Python object.
        recursively_ensure_json_conversion (bool, optional): If True, the object will be
            recursively converted via ``dict__``. Only used when ``converter`` is None.
            Defaults to True.
        converter (Union[JsonConverter, str, Callable, None], optional):
            Controls how the object is converted before serialization.
        parts_root_path (str, optional): The resolved file path used to determine
            the ``.parts/`` directory location (i.e., ``parts_root_path + parts_suffix``).
            When None, parts extraction is skipped. Defaults to None.
        parts_key_paths: Key paths to extract as separate files. Each entry can be:
            - A plain string: 'item'
            - A 2-tuple: ('body_html', '.html')
            - A 3-tuple with name alias: ('body_html', '.html', 'BodyHtml')
            - A 4-tuple with subfolder: ('body_html', '.html', None, 'ui_source/html')
            - A PartsKeyPath instance: PartsKeyPath('body_html', ext='.html', subfolder='ui_source/html')
            Entries can be freely mixed. Also accepts the string '*' to extract all
            top-level keys, or None (default) to disable extraction.
        parts_min_size (int, optional): Minimum serialized size (in chars) to trigger extraction.
            Values smaller than this are left inline. Defaults to 0.
        parts_mode: What replaces extracted values in the main dict.
            One of PartsReplacementMode values. Defaults to 'reference'.
        parts_suffix (str, optional): Suffix for the parts directory. Defaults to '.parts'.
        parts_preview_len (int, optional): Max chars for inline preview in TRUNCATE mode. Defaults to 200.
        parts_preview_with_path (bool, optional): When True with path/filename modes,
            include a truncated preview after the path. Defaults to False.
        parts_path_as_url (bool, optional): When True with path/filename modes,
            prefix with file:///. Defaults to False.
        parts_file_namer (Callable, optional): Custom namer for parts files.
        parts_subfolder (str, optional): Subdirectory within the parts directory.
        parts_group_by_key (bool, optional): When True, parts files are organized into
            subdirectories named by the key path (or its alias). Defaults to False.
        parts_key_path_root (str, optional): Prefix for all key paths during extraction.
        artifacts_as_parts: Merge @artifact_field-decorated fields into
            parts_key_paths. True for all groups, or a list of group names.
        leaf_as_parts_if_exceeding_size (int, optional): When set, any leaf string value in the
            dict exceeding this character length is automatically extracted to a parts
            file. Runs after explicit ``parts_key_paths`` extraction (already-extracted
            paths are skipped). Defaults to None (disabled).
        is_artifact (bool or tuple of types, optional): Convenience shorthand
            for ``parts_key_paths='*'``.  Only takes effect when ``parts_key_paths``
            is not already set.
            - ``True``: extract all fields regardless of type.
            - A tuple of types, e.g. ``(str,)``: extract all fields only when the
              content (at ``parts_key_path_root`` if set, otherwise ``obj`` itself)
              is an instance of one of the given types.
            Defaults to False.
        overwrite (bool, optional): If True, clears the parts directory before writing.
            If False, parts files accumulate. Defaults to True.
        ensure_ascii (bool, optional): Passed to _serialize_value for parts content.
            Defaults to False.

    Returns:
        The converted (and possibly parts-extracted) object, or None if obj was None.
    """
    if obj is None:
        return

    # Convenience shorthand: is_artifact → extract all fields
    if is_artifact and parts_key_paths is None:
        if is_artifact is True:
            parts_key_paths = '*'
        else:
            # is_artifact is a tuple of types — check the content type
            _check_val = obj.get(parts_key_path_root) if parts_key_path_root and isinstance(obj, dict) else obj
            if isinstance(_check_val, is_artifact):
                parts_key_paths = '*'

    # Capture original type before dict conversion (for artifacts_as_parts)
    _original_type = type(obj) if artifacts_as_parts else None

    if converter is not None:
        if callable(converter) and not isinstance(converter, (JsonConverter, str)):
            obj = converter(obj)
        elif converter in (JsonConverter.DICT, 'dict'):
            if not isinstance(obj, Mapping):
                obj = dict__(obj, recursive=True)
        elif converter in (JsonConverter.DICT_NON_RECURSIVE, 'dict_non_recursive'):
            if not isinstance(obj, Mapping):
                obj = dict__(obj, recursive=False)
        elif converter in (JsonConverter.STR, 'str'):
            obj = str(obj)
        elif converter in (JsonConverter.NONE, 'none'):
            pass
        else:
            raise ValueError(f"Unknown converter: {converter!r}")
    else:
        if not isinstance(obj, Mapping):
            obj = dict__(obj, recursive=recursively_ensure_json_conversion)

    # Merge artifact-decorated fields into parts_key_paths
    if artifacts_as_parts:
        groups = None if artifacts_as_parts is True else artifacts_as_parts

        # Determine the type to inspect for @artifact_field decorators.
        # When parts_key_path_root is set, the actual item lives at that path
        # (e.g. log_data['item']), so resolve the type from there.
        _artifact_type = _original_type
        if parts_key_path_root and has_path(obj, parts_key_path_root):
            _artifact_type = type(get_at_path(obj, parts_key_path_root))

        artifact_entries = get_key_paths_for_artifacts(_artifact_type, groups=groups)

        if artifact_entries:
            if parts_key_paths:
                # Deduplicate: explicit parts_key_paths take precedence over artifact entries
                explicit_keys = {
                    _normalize_extract_path_entry(e)[0] for e in parts_key_paths
                }
                artifact_entries = [e for e in artifact_entries if e.key not in explicit_keys]
            parts_key_paths = artifact_entries + (list(parts_key_paths) if parts_key_paths else [])

    # Ensure nested objects at parts_key_path_root are dict-converted
    # so has_path/get_at_path can navigate into them during extraction.
    if parts_key_path_root and parts_key_paths and isinstance(obj, dict):
        _inner = obj.get(parts_key_path_root)
        if _inner is not None and not isinstance(_inner, (dict, list, str, int, float, bool, type(None), bytes)):
            obj = dict(obj)  # shallow copy to avoid mutating caller's dict
            obj[parts_key_path_root] = dict__(_inner, recursive=True)

    # Extract large values to .parts/ files (only when parts_root_path is set)
    _deepcopied = False
    if parts_key_paths is not None and parts_root_path is not None and isinstance(obj, dict):
        # Expand wildcard
        if parts_key_paths == '*':
            if parts_key_path_root:
                _root_val = obj.get(parts_key_path_root)
                if isinstance(_root_val, dict):
                    # Dict at root — expand to its keys (will be prefixed in the loop)
                    parts_key_paths = list(_root_val.keys())
                elif _root_val is not None:
                    # Scalar value at root — extract the root path itself
                    parts_key_paths = [parts_key_path_root]
                    parts_key_path_root = None  # path is already absolute, don't prefix
                else:
                    parts_key_paths = []  # root not found
            else:
                parts_key_paths = list(obj.keys())

        obj = copy.deepcopy(obj)
        _deepcopied = True

        mode = PartsReplacementMode(parts_mode)
        base_parts_dir = parts_root_path + parts_suffix
        parts_dir = os.path.join(base_parts_dir, parts_subfolder) if parts_subfolder else base_parts_dir

        # Clear parts directory if overwriting
        if overwrite and os.path.isdir(parts_dir):
            shutil.rmtree(parts_dir)

        for entry in parts_key_paths:
            path_str, ext_override, name_alias, entry_subfolder = _normalize_extract_path_entry(entry)
            original_path_str = path_str  # for file naming (un-prefixed)

            # Apply root prefix for data access
            if parts_key_path_root:
                path_str = f"{parts_key_path_root}.{path_str}"

            if not has_path(obj, path_str):
                continue
            value = get_at_path(obj, path_str)
            if value is None:
                continue

            serialized = _serialize_value(value, ensure_ascii=ensure_ascii)
            if len(serialized) < parts_min_size:
                continue

            # Determine extension (normalize: 'html' → '.html')
            ext = resolve_ext(ext_override) if ext_override else _detect_extension(value)

            # Build filename with timestamp+uuid for uniqueness
            file_stem = name_alias if name_alias else original_path_str.replace('.', '__')
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            uid = uuid.uuid4().hex[:8]
            name_hint = parts_file_namer(obj) if parts_file_namer is not None else None
            segments = [ts]
            if name_hint:
                segments.append(name_hint)
            if file_stem and file_stem != 'item':
                segments.append(file_stem)
            segments.append(uid)
            filename = '_'.join(segments) + ext

            # Write to parts file
            entry_dir = parts_dir
            if entry_subfolder:
                entry_dir = path.join(entry_dir, entry_subfolder)
            if parts_group_by_key:
                entry_dir = path.join(entry_dir, file_stem)
            os.makedirs(entry_dir, exist_ok=True)
            parts_file_path = path.join(entry_dir, filename)
            with open(parts_file_path, 'w', encoding='utf-8') as pf:
                pf.write(serialized)

            # Path from parts_dir to the file (accounts for entry_subfolder and parts_group_by_key)
            rel_segments = []
            if entry_subfolder:
                rel_segments.append(entry_subfolder)
            if parts_group_by_key:
                rel_segments.append(file_stem)
            rel_segments.append(filename)
            rel_to_parts_dir = path.join(*rel_segments)

            # Replace value in obj based on mode
            if mode == PartsReplacementMode.REMOVE:
                delete_at_path(obj, path_str)
            elif mode == PartsReplacementMode.TRUNCATE:
                parts_ref = path.join(parts_subfolder, rel_to_parts_dir) if parts_subfolder else rel_to_parts_dir
                truncated = serialized[:parts_preview_len] + f'...[truncated, see {parts_ref}]'
                set_at_path(obj, path_str, truncated)
            elif mode in (PartsReplacementMode.ABSOLUTE_PATH,
                          PartsReplacementMode.RELATIVE_PATH,
                          PartsReplacementMode.FILENAME_ONLY):
                if mode == PartsReplacementMode.ABSOLUTE_PATH:
                    ref_path = path.abspath(parts_file_path)
                elif mode == PartsReplacementMode.RELATIVE_PATH:
                    base_parts_name = path.basename(base_parts_dir)
                    if parts_subfolder:
                        ref_path = path.join(base_parts_name, parts_subfolder, rel_to_parts_dir)
                    else:
                        ref_path = path.join(base_parts_name, rel_to_parts_dir)
                else:
                    ref_path = filename

                if parts_path_as_url:
                    ref_path = 'file:///' + ref_path

                if parts_preview_with_path:
                    ref_path = ref_path + '\n---\n' + serialized[:parts_preview_len] + '...'

                set_at_path(obj, path_str, ref_path)
            else:
                # REFERENCE mode (default)
                value_type = 'str' if isinstance(value, str) else 'json'
                set_at_path(obj, path_str, {
                    '__parts_file__': path.join(parts_subfolder, rel_to_parts_dir) if parts_subfolder else rel_to_parts_dir,
                    '__value_type__': value_type,
                })

    # Auto-extract oversized leaf string values
    if leaf_as_parts_if_exceeding_size is not None and parts_root_path is not None and isinstance(obj, dict):
        if not _deepcopied:
            obj = copy.deepcopy(obj)

        mode = PartsReplacementMode(parts_mode)
        base_parts_dir = parts_root_path + parts_suffix
        _leaf_parts_dir = os.path.join(base_parts_dir, parts_subfolder) if parts_subfolder else base_parts_dir

        for key_path, value in obj_walk_through(obj):
            if not isinstance(value, str):
                continue
            if len(value) < leaf_as_parts_if_exceeding_size:
                continue

            path_str = '.'.join(key_path)
            file_stem = path_str.replace('.', '__')

            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            uid = uuid.uuid4().hex[:8]
            ext = _detect_extension(value)
            name_hint = parts_file_namer(obj) if parts_file_namer is not None else None
            segments = [ts]
            if name_hint:
                segments.append(name_hint)
            if file_stem and file_stem != 'item':
                segments.append(file_stem)
            segments.append(uid)
            filename = '_'.join(segments) + ext

            os.makedirs(_leaf_parts_dir, exist_ok=True)
            parts_file_path = path.join(_leaf_parts_dir, filename)
            with open(parts_file_path, 'w', encoding='utf-8') as pf:
                pf.write(value)

            rel_to_parts_dir = filename

            if mode == PartsReplacementMode.REMOVE:
                delete_at_path(obj, path_str)
            elif mode == PartsReplacementMode.TRUNCATE:
                parts_ref = path.join(parts_subfolder, rel_to_parts_dir) if parts_subfolder else rel_to_parts_dir
                truncated = value[:parts_preview_len] + f'...[truncated, see {parts_ref}]'
                set_at_path(obj, path_str, truncated)
            elif mode in (PartsReplacementMode.ABSOLUTE_PATH,
                          PartsReplacementMode.RELATIVE_PATH,
                          PartsReplacementMode.FILENAME_ONLY):
                if mode == PartsReplacementMode.ABSOLUTE_PATH:
                    ref_path = path.abspath(parts_file_path)
                elif mode == PartsReplacementMode.RELATIVE_PATH:
                    base_parts_name = path.basename(base_parts_dir)
                    if parts_subfolder:
                        ref_path = path.join(base_parts_name, parts_subfolder, rel_to_parts_dir)
                    else:
                        ref_path = path.join(base_parts_name, rel_to_parts_dir)
                else:
                    ref_path = filename
                if parts_path_as_url:
                    ref_path = 'file:///' + ref_path
                if parts_preview_with_path:
                    ref_path = ref_path + '\n---\n' + value[:parts_preview_len] + '...'
                set_at_path(obj, path_str, ref_path)
            else:
                set_at_path(obj, path_str, {
                    '__parts_file__': path.join(parts_subfolder, rel_to_parts_dir) if parts_subfolder else rel_to_parts_dir,
                    '__value_type__': 'str',
                })

    return obj


def write_json(
        obj,
        file_path: str,
        append: bool = False,
        indent=None,
        create_dir=True,
        encoding='utf-8',
        ensure_ascii: bool = False,
        subfolder: str = None,
        space_ext_mode: Union[SpaceExtMode, str, bool, None] = None,
        **kwargs
):
    """
    Writes a JSON representation of an object to a file.

    This function first converts the object to JSON-serializable form via
    :func:`jsonfy`, then writes the result to a file. Any keyword arguments
    matching :func:`jsonfy` parameters (e.g. ``converter``, ``parts_key_paths``,
    ``artifacts_as_parts``) are forwarded automatically.

    Args:
        obj: The object to write to the file. Can be any Python object.
        file_path (str): The path to the file where the JSON data will be written.
        append (bool, optional): If True, data will be appended to the file. If False, the file
            will be overwritten. Defaults to False.
        indent (int, optional): If specified, JSON output will be indented by this number of spaces.
        create_dir (bool, optional): If True, the directory specified in `file_path` will be created
            if it does not exist. Defaults to True.
        encoding (str, optional): The encoding to use for the output file. Defaults to 'utf-8'.
        ensure_ascii (bool, optional): If True, ensure that the output is ASCII-only. Defaults to False.
        subfolder (str, optional): Inserts a subdirectory between the parent folder
            and filename of file_path. Useful when write_json is wrapped in a partial
            and the caller cannot modify file_path directly.
            E.g. file_path='logs/session.jsonl', subfolder='iter_0001'
            → 'logs/iter_0001/session.jsonl'. Defaults to None (no insertion).
        space_ext_mode (SpaceExtMode, str, bool, or None): Controls how the file
            extension interacts with the space parameter. Only applies when space is set.
            - SpaceExtMode.NONE / None / False: No modification (default).
            - SpaceExtMode.MOVE / True: Move the extension from file_path to space.
              file_path='log.jsonl', space='id' → file='log/id.jsonl'
            - SpaceExtMode.ADD: Append the extension to space without removing it
              from file_path. file_path='log.jsonl', space='id' → file='log.jsonl/id.jsonl'
        **kwargs: Additional keyword arguments forwarded to :func:`jsonfy`,
            ``open_``, and ``json.dumps``.

    Returns:
        The converted object (post-jsonfy).

    Examples:
        Write a dictionary to a JSON file:

        >>> import tempfile
        >>> import os
        >>> import json
        >>> tmp_file = tempfile.NamedTemporaryFile(delete=False)
        >>> tmp_name = tmp_file.name
        >>> tmp_file.close()  # Close the file first to release the lock
        >>> write_json({'a': 1, 'b': 2}, tmp_name)
        overwrite file ...
        >>> with open(tmp_name, 'r') as f:
        ...    print(f.read())
        {"a": 1, "b": 2}
        >>> os.unlink(tmp_name)

    """
    if obj is None:
        return

    if create_dir:
        ensure_dir_existence(path.dirname(file_path), verbose=False)

    # Insert subfolder between parent dir and filename
    if subfolder is not None:
        parent, filename = path.dirname(file_path), path.basename(file_path)
        file_path = path.join(parent, subfolder, filename)

    # Resolve actual file path (accounts for space + space_ext_mode)
    _resolved_file_path = file_path
    space = kwargs.get('space')
    if space is not None and space_ext_mode not in (None, False, SpaceExtMode.NONE, 'none'):
        # Normalize True to MOVE for backward compatibility
        mode = SpaceExtMode.MOVE if space_ext_mode is True else SpaceExtMode(space_ext_mode)
        base, ext = os.path.splitext(file_path)
        if mode == SpaceExtMode.MOVE:
            # Move ext from file_path to space: 'log.jsonl' → dir='log', space='id.jsonl'
            _resolved_file_path = base
            space = space + ext
            kwargs['space'] = space
            file_path = _resolved_file_path
        elif mode == SpaceExtMode.ADD:
            # Add ext to space, keep file_path as-is: space='id' → space='id.jsonl'
            space = space + ext
            kwargs['space'] = space
    if space is not None:
        space_handler = kwargs.get('space_handler', _default_space_handler)
        _resolved_file_path = space_handler(file_path, space)

    # Split kwargs: jsonfy params vs remaining (open_ + json.dumps)
    jsonfy_kwargs, other_kwargs = get_relevant_named_args(
        jsonfy,
        return_other_args=True,
        **kwargs
    )

    # Convert and extract parts
    obj = jsonfy(
        obj,
        parts_root_path=_resolved_file_path,
        overwrite=not append,
        ensure_ascii=ensure_ascii,
        **jsonfy_kwargs
    )

    # Split remaining kwargs: open_ params vs json.dumps params
    kwargs_open_explicitly_named, kwargs_others = get_relevant_named_args(
        open_.__init__,
        return_other_args=True,
        **other_kwargs
    )

    with open_(file_path, 'a' if append else 'w', encoding=encoding, **other_kwargs) as fout:
        fout.write(
            json.dumps(obj, indent=indent, ensure_ascii=ensure_ascii, **kwargs_others)
            if encoding
            else json.dumps(obj, indent=indent, **other_kwargs)
        )
        fout.write('\n')
        fout.flush()

    return obj

from functools import partial
from os import path
import glob
from typing import List, Optional

from rich_python_utils.common_utils.iter_helper import chunk_iters
from rich_python_utils.io_utils.json_io import iter_json_objs, iter_all_json_objs_from_all_sub_dirs, write_json_objs

chunk_size = 600
input_path_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data'
output_path = f'/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_{chunk_size}'


def glob_paths(path_pattern: str) -> List[str]:
    if path.isfile(path_pattern):
        return [path_pattern]

    if path.isdir(path_pattern):
        path_pattern = path.join(path_pattern, '*')
    return glob.glob(path_pattern)


input_paths = glob_paths(input_path_root)


def is_dds_data_entry(jobj: dict) -> Optional[bool]:
    turns = jobj.get('turns', None)
    if turns:
        if isinstance(turns, list):
            return isinstance(turns[0], dict)


def is_dds_data_file(input_file_path: str) -> Optional[bool]:
    try:
        first_jobj = next(iter_json_objs(input_file_path))
    except:
        return None
    return is_dds_data_entry(first_jobj)


KEY_DIALOG_ID = 'dialog_id'

input_dds_data_files = []
input_non_dds_data_files = []
for input_path in input_paths:
    is_dds = is_dds_data_file(input_path)
    if is_dds is None:
        raise ValueError()
    else:
        if is_dds:
            input_dds_data_files.append(input_path)
        else:
            input_non_dds_data_files.append(input_path)


def iter_json_objs_with_added_file_tags(data_file):
    for data_index, data_entry in enumerate(iter_json_objs(data_file)):
        yield {**data_entry, '_data_file': data_file, '_data_index': data_index}


def dds_data_entry_key(x):
    return x[KEY_DIALOG_ID] + '_' + x['turns'][0]['utterance']


for chunk_index, chunk in enumerate(chunk_iters(
        iterables=[
            iter_json_objs_with_added_file_tags(data_file)
            for data_file in input_dds_data_files
        ],
        chunk_size=chunk_size,
        group_key=dds_data_entry_key,
        group_sort=partial(sorted, key=lambda x: len(x['turns']))
)):
    write_json_objs(chunk, path.join(output_path, f'dds_data_{chunk_index}.json'))

for chunk_index, chunk in enumerate(chunk_iters(
        iterables=[
            iter_json_objs_with_added_file_tags(data_file)
            for data_file in input_non_dds_data_files
        ],
        chunk_size=chunk_size
)):
    write_json_objs(chunk, path.join(output_path, f'non_dds_data_{chunk_index}.json'))

output_path_dds_data_files = glob.glob(path.join(output_path, 'dds_data_*'))
dialog_ids_visited = {}
for output_path_dds_data_file in output_path_dds_data_files:
    for jobj in iter_json_objs(output_path_dds_data_file):
        dialog_id = (jobj['_data_file'], dds_data_entry_key(jobj))
        if dialog_id in dialog_ids_visited:
            if dialog_ids_visited[dialog_id][0] != output_path_dds_data_file:
                raise ValueError()
        else:
            dialog_ids_visited[dialog_id] = (output_path_dds_data_file, jobj)

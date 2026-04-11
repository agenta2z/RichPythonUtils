import glob
from collections import defaultdict
from os import path

from rich_python_utils.io_utils.json_io import iter_all_json_objs_from_all_sub_dirs, write_json_objs
from rich_python_utils.path_utils.path_string_operations import get_main_name

input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_2000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'all_0626_claude3_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3_by_dataset'


input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_200_shopping_only'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'shopping_0627_claude3_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/shopping_claude3_by_dataset'

id_to_file_map = {}
for jobj in iter_all_json_objs_from_all_sub_dirs(input_path_dataset_root):
    data_file = jobj['_data_file']
    data_index = jobj['_data_index']
    if 'dialog_id' in jobj:
        dialog_id = jobj['dialog_id']
        turns = jobj['turns']
        for turn in turns:
            turn_id = turn['turn_id']
            id = f'{dialog_id}-{turn_id}'
            id_to_file_map[id] = {
                '_data_file': data_file,
                '_data_index': data_index
        }


restored_datasets = defaultdict(list)

for jobj in iter_all_json_objs_from_all_sub_dirs(input_paths_results):
    if '_data_file' in jobj:
        _data_file = jobj['_data_file']
        _data_index = jobj['_data_index']
    else:
        dialog_id = jobj['dialog_id']
        turn_id = jobj['turn_id']
        id = f'{dialog_id}-{turn_id}'
        if id not in id_to_file_map:
            raise ValueError()
        else:
            _data_file = id_to_file_map[id]['_data_file']
            _data_index = id_to_file_map[id]['_data_index']
    jobj['_data_index'] = _data_index
    jobj['_data_file'] = _data_file
    restored_datasets[_data_file].append((_data_index, jobj))

for _data_file, results in restored_datasets.items():
    dataset_name = get_main_name(_data_file)
    file_name = path.basename(_data_file)
    write_json_objs(list(zip(*results))[1], path.join(output_path_results_root, dataset_name, 'post-process', file_name))
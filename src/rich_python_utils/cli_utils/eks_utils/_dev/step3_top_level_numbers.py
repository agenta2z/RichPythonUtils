from collections import defaultdict

from camel_llm_prompter._dev.top_level.evaluation.planning.utils import get_experts_for_action_reference
from camel_llm_prompter._dev.top_level.utils.config_utils import get_config_path
from rich_python_utils.common_utils import save_to_csv
from rich_python_utils.common_utils.iter_helper import flatten_iter
from rich_python_utils.common_utils.map_helper import get_value_by_path
from rich_python_utils.common_utils.misc import divide_
from rich_python_utils.io_utils.json_io import iter_json_objs, iter_all_json_objs_from_all_sub_dirs, write_json_objs
import glob
import json

from rich_python_utils.path_utils.path_string_operations import get_main_name

input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_500-multiturn'

id_to_file_map = {}
for jobj in iter_all_json_objs_from_all_sub_dirs(input_path_dataset_root):
    data_file = jobj['_data_file']
    data_index = jobj['_data_index']
    dialog_id = jobj['dialog_id']
    turns = jobj['turns']
    for turn in turns:
        turn_id = turn['turn_id']
        id = f'{dialog_id}-{turn_id}'
        id_to_file_map[id] = {
            '_data_file': data_file,
            '_data_index': data_index
        }

from os import path
input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_api_planning_verbose_multi_turn_only.yaml/v17-OP6'
input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_api_planning_verbose_multi_turn_only.yaml/claude3'
input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_api_planning_verbose_multi_turn_only.yaml/ra8bv1'
input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_api_planning_verbose_multi_turn_only.yaml/ra8bv2'
input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_top_level_multi_turn_api_planning_verbose_multi_turn_only.yaml/ra8bv1'
input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_top_level_multi_turn_api_planning_verbose_multi_turn_only.yaml/ra8bv2'
input_path_yaml ='/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_api_planning_verbose_multi_turn_only.yaml/claude3'
input_path_root = f'{input_path_yaml}/multiturn*/*/post-process/*.jsonl'


# input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_top_level_multi_turn_api_planning_verbose_multi_turn_only.yaml/claude3'
# '/data/meta-reasoning-sandbox/evals/e2e_0621/config_top_level_multi_turn_api_planning_verbose_multi_turn_only.yaml/v17-OP6/multiturn'
# input_path_yaml = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_top_level_multi_turn_api_planning_verbose_multi_turn_only.yaml/v17-OP6/multiturn'
# input_path_root = f'{input_path_yaml}/multiturn/*/*/post-process/*.jsonl'


output_path = path.join(input_path_yaml + '_analysis', 'top_level_acc.csv')
output_path_rd = path.join(input_path_yaml + '_analysis', 'regrouped_post_process')

input_paths = glob.glob(input_path_root)

API_GROUNDING_EXPERTS_API_ASSIGNMENT = json.load(
    open(get_config_path('top_level/planning/v1/expert_info.json'))
)

regrouped_data = defaultdict(list)

empty_output = 0
non_empty_count = 0  # 4438
non_empty_top_level_count = 0  # 4438
metrics = defaultdict(lambda: [0, 0])
visisted_id = set()
for jobj in iter_all_json_objs_from_all_sub_dirs(input_paths):
    if not jobj['reasoning_outputs']:
        empty_output += 1
    else:
        non_empty_count += 1
        top_level_output = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning'])
        if top_level_output is not None:
            non_empty_top_level_count += 1

            reference = jobj['reference']
            target_experts = list(flatten_iter(get_experts_for_action_reference(reference, API_GROUNDING_EXPERTS_API_ASSIGNMENT)))
            top_level_module_name = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning', 'output', 'moduleName'])

            if target_experts:
                dialog_id = jobj['dialog_id']
                turn_id = jobj['turn_id']
                id = f'{dialog_id}-{turn_id}'
                if id not in visisted_id:
                    visisted_id.add(id)
                    if id in id_to_file_map:
                        data_file = id_to_file_map[id]['_data_file']
                        regrouped_data[get_main_name(data_file)].append(jobj)
                        metrics[data_file][1] += 1
                        if top_level_module_name in target_experts:
                            metrics[data_file][0] += 1
out = []
for k, v in metrics.items():
    out.append([get_main_name(k), *v, divide_(v[0], v[1])])

from pprint import pprint
pprint(sorted(out))

save_to_csv(sorted(out), output_path)

for k, v in regrouped_data.items():
    write_json_objs(v, path.join(output_path_rd, k + '.json'))
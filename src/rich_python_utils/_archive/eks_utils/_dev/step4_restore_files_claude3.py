import glob
from collections import defaultdict
from os import path

from rich_python_utils.io_utils.json_io import iter_all_json_objs_from_all_sub_dirs, write_json_objs
from rich_python_utils.path_utils.path_string_operations import get_main_name

input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_2000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'all_0626_claude3_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3_by_dataset'

input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1667_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0626_claude35_*/*/post-process/*.jsonl'))
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0623/config_claude35.yaml/claude3'
input_paths_results.extend(glob.glob(path.join(input_path_results_root, 'core_0626_claude35_*/*/post-process/*.jsonl')))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_by_dataset'

input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_3000_exclude_0212c2b8bd2a3985c8c79afe2f1f6853fa2190d8'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'non_core_0626_claude35_*/*/post-process/*.jsonl'))
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0623/config_claude35.yaml/claude3'
input_paths_results.extend(glob.glob(path.join(input_path_results_root, 'non_core_0626_claude35_*/*/post-process/*.jsonl')))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_non_core_by_dataset'


input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_2000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0623/config_top_level_multi_turn_api_planning_verbose_multi_turn_only.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'multi_turn_0626_claude35_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_multi_turn_pre0626_v2_by_dataset'

input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_2000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_api_planning_verbose_multi_turn_only_jun26.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'multi_turn_0626_claude35_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_multi_turn_by_dataset'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0625/config_api_planning_verbose_multi_turn_only_jun26.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'multi_turn_0626_claude3_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude3_multi_turn_by_dataset'



input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_2000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0625/config_claude3.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'non_core_0626_claude3_*/*/post-process/*.jsonl*'))
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0624/config_claude3.yaml/claude3'
input_paths_results.extend(glob.glob(path.join(input_path_results_root, 'core_0626_c3_*/*/post-process/*.jsonl*')))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_claude3.yaml/final_results/all_0626'


input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0625_haiku/config_claude3.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'all_0626_c3h_*/*/post-process/*.json*'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0625_haiku/config_claude3.yaml/restored_results/claude3_haiku-0626'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_pre0626.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'all_pre0626_c3h_*/*/post-process/*.json*'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_haiku_pre0626.yaml/final_results/all_0626'




input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1667_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0707.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707.yaml/final_results/claude3_haiku-core_0707'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_dev/config_claude3_0707.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707_c3_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_0707.yaml/final_results/claude3-core_0707'





input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_300_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0709_dev/config_claude35_self_learn_shopping.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'sl_sp_0707_c35_*/*/post-process/*.json*'))
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0710_dev/config_claude35_self_learn_shopping.yaml/claude3'
input_paths_results.extend(glob.glob(path.join(input_path_results_root, 'sl_sp_0707_c35_*/*/post-process/*.json*')))
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0710_dev3/config_claude35_self_learn_shopping.yaml/claude3'
input_paths_results.extend(glob.glob(path.join(input_path_results_root, 'sl_sp_0707_c35_*/*/post-process/*.json*')))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude35_self_learn_shopping.yaml/final_results/claude35-self_learning_shopping_rule'




input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_500-multiturn'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev/config_mt_claude3_haiku.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'mt_0707_c3h_r2_*/*/post-process/*.json*'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev/config_mt_claude3_haiku.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'mt_0707_c3h_r4_*/*/post-process/*.json*'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run2'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_mt_claude3_haiku.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'mt_0707_c3h_r5_*/*/post-process/*.json*'))
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_mt_claude3_haiku.yaml/claude3'
input_paths_results.extend(glob.glob(path.join(input_path_results_root, 'mt_0707_c3h_r5_*/*/post-process/*.json*')))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run3'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_mt_claude3_haiku.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'mt_0707_c3h_r7_*/*/post-process/*.json*'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run4'



input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1000_include_427077dc7f35ea0e1ed4da16297f874c0ae095d8'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_claude3_haiku_0707.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707v2_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707.yaml/claude3_haiku-core_0707v2'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_claude3_haiku_0707_no_top_level_rules.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707v2nr_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_no_top_level_rules.yaml/claude3_haiku-core_0707v2'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0707_slsp.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707v2nde_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_slsp.yaml/claude3_haiku-core_0707v2'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0707_no_top_level_dynamic_exemplars.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707v2nde_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_no_top_level_dynamic_exemplars.yaml/claude3_haiku-core_0707v2'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_claude3_haiku_0707_slsp_ndexp.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'core_0707v2nde_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_slsp_ndexp.yaml/claude3_haiku-core_0707v2'


input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0711/test_sets_managed/source_data_chunked/chunk_1000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0711_prod_with_fallback.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0711_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback.yaml/claude3_haiku-prod0711'



input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0712/test_sets_managed/source_data_chunked/chunk_1000'
input_path_results_root = '/data/khababer-sandbox/logs/e2e_0707_haiku_dev/all_data_eval/expert_mapping_v3_xml_prompt_with_fallback_5experts_chunked'
input_paths_results = glob.glob(path.join(input_path_results_root, '*/*/post-process/*.jsonl'))
output_path_results_root = '/data/khababer-sandbox/logs/e2e_0707_haiku_dev/all_data_eval/expert_mapping_v3_xml_prompt_with_fallback_5experts'


input_path_dataset_root ='/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1500'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0707_c3h_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3_haiku-prod0707iq'


input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0712/test_sets_managed/source_data_chunked/chunk_1000'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r4_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r5_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r5'


input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r7_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r7'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev2/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r10_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r10'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r11_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r11'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev3/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r12_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r12'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev5/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r13_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r12'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev5/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r13_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r13'

input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev4/config_claude3_haiku_0716_prod_with_fallback_and_added_tags.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r13_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r13_with_added_tags'


input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev4/config_claude3_haiku_0716_prod_with_fallback_and_added_tags.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0712iq_c3h_r14_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq_r14_with_added_tags'


input_path_dataset_root = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0717/wave1_wave2_combined_testsets/source_data_chunked/chunk_1200'
input_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev6/config_claude3_haiku_wave2_prod_with_fallback_0716_v2.yaml/claude3'
input_paths_results = glob.glob(path.join(input_path_results_root, 'prod0717_c3h_r3_*/*/post-process/*.jsonl'))
output_path_results_root = '/data/meta-reasoning-sandbox/evals/e2e_0717/config_claude3_haiku_wave2_prod_with_fallback_0716_v2.yaml/final_results/claude3_haiku-prod0717'


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
    else:
        id = '\n'.join(y['utterance'] for y in jobj['turns'])
        id_to_file_map[id] = {
            '_data_file': data_file,
            '_data_index': data_index
        }

restored_datasets = defaultdict(list)

miss = 0
for jobj in iter_all_json_objs_from_all_sub_dirs(input_paths_results):
    if '_data_file' in jobj:
        _data_file = jobj['_data_file']
        _data_index = jobj['_data_index']
    else:
        if 'dialog_id' in jobj:
            dialog_id = jobj['dialog_id']
            if 'turn_id' not in jobj:
                miss += 1
                continue
            turn_id = jobj['turn_id']
            id = f'{dialog_id}-{turn_id}'
        else:
            id = jobj['utterance']

        if id in id_to_file_map:
            _data_file = id_to_file_map[id]['_data_file']
            _data_index = id_to_file_map[id]['_data_index']
        else:
            miss += 1
            continue
    jobj['_data_index'] = _data_index
    jobj['_data_file'] = _data_file
    restored_datasets[_data_file].append((_data_index, jobj))

for _data_file, results in restored_datasets.items():
    dataset_name = get_main_name(_data_file)
    file_name = path.basename(_data_file)
    write_json_objs(list(zip(*results))[1], path.join(output_path_results_root, dataset_name, 'post-process', file_name))

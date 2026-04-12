from collections import defaultdict
from typing import Sequence

from rich_python_utils.common_utils.array_helper import save_to_csv
from rich_python_utils.common_utils.misc import divide_
from rich_python_utils.io_utils.json_io import iter_json_objs, iter_all_json_objs_from_all_sub_dirs, write_json_objs

from camel_llm_prompter._dev.top_level.evaluation.planning.utils import get_experts_for_action_reference
from camel_llm_prompter._dev.top_level.utils.config_utils import get_config_path
from camel_llm_prompter._dev.top_level.utils.data_utils import get_reference, get_conversation_from_post_processed_data, ReferenceTypes
from rich_python_utils.common_utils import get_value_by_path
from rich_python_utils.common_utils.iter_helper import flatten_iter
from rich_python_utils.console_utils import hprint_message, eprint_message
from rich_python_utils.io_utils.json_io import iter_json_objs, iter_all_json_objs_from_all_sub_dirs, write_json_objs
import json
import glob
from os import path

from rich_python_utils.path_utils.path_string_operations import get_main_name
from rich_python_utils.string_utils.parsing import parse_function_call

# API_GROUNDING_EXPERTS_API_ASSIGNMENT = json.load(
#    open(get_config_path('top_level/planning/v1/expert_info_prod0711.json'))
# )

API_GROUNDING_EXPERTS_API_ASSIGNMENT = json.load(open("/data/meta-reasoning-sandbox/data/evaluation_datasets/0719/mr_wave1_wave2_train3_0719_2pm/expert_mapping_wave1_wave2_train3_0719_2pm.json"))
all_target_experts = [x['expert_name'] for x in API_GROUNDING_EXPERTS_API_ASSIGNMENT]

# Multi-turn Flags
compute_metrics_for_multi_turn_only = True
compute_metrics_for_single_turn_only = False

# input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback.yaml/claude3_haiku-prod0711'
# input_paths = glob.glob(path.join(input_path_root, '*/post-process/*.json*'))

input_path_root = '/data/khababer-sandbox/logs/e2e_0707_haiku_dev/all_data_eval/expert_mapping_v3_xml_prompt'
# input_path_root = '/data/khababer-sandbox/logs/e2e_0707_haiku_dev/all_data_eval/expert_mapping_v3_xml_prompt_info_expert_0713_small_data'
# input_path_root = '/data/khababer-sandbox/logs/e2e_0707_haiku_dev/all_data_eval/expert_mapping_v4_xml_prompt_new_experts_noexpfallback_0713_sm_data'
# input_path_root = '/data/khababer-sandbox/logs/e2e_0707_haiku_dev/all_data_eval/expert_mapping_v4_infoqa_plus_exp_prompt_no_observation'
# input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml/final_results/claude3_haiku-prod0712iq'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0717/config_claude3_haiku_wave2_prod_with_fallback_0716_v2.yaml/final_results/claude3_haiku-prod0717'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev6/config_claude3_haiku_wave2_prod_with_fallback_0716_v2.yaml/claude3'
input_path_root = '/data/khababer-sandbox/logs/0718_sandbox_evaluation_2PM/all_data_eval/evaluation_wave2_prompt_wave1_data'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev6/config_claude3_haiku_wave2_prod_no_fallback_0716_v2.yaml/claude3'
input_path_root = '/data/khababer-sandbox/logs/0718_sandbox_evaluation_2PM/all_data_eval/evaluation_wave2_proactive_expert_prompt_0719_3PM'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707_haiku_dev6/config_claude3_haiku_wave2_prod_no_fallback_0716_v3.yaml/claude3'
input_path_root = '/data/khababer-sandbox/logs/0718_sandbox_evaluation_2PM/all_data_eval/evaluation_wave2_proactive_expert_prompt_0719_10PM'

input_paths = glob.glob(path.join(input_path_root, '*/*/post-process/*.json*'))

hprint_message('input_paths_len', len(input_paths))

output_path_suffix = '_analysis_v3'
if compute_metrics_for_multi_turn_only:
    output_path_suffix += '_mturn_only'
elif compute_metrics_for_single_turn_only:
    output_path_suffix += '_sturn_only'
output_path_root = input_path_root + output_path_suffix
output_path_mismatched_test_cases_root = path.join(output_path_root, 'mismatched_test_cases')
output_path_matched_test_cases_root = path.join(output_path_root, 'matched_test_cases')

dataset_metrics = defaultdict(lambda: defaultdict(int))
expert_metrics = defaultdict(lambda: defaultdict(int))
api_metrics = defaultdict(lambda: defaultdict(int))
mismatched_test_cases_by_expert = defaultdict(lambda: defaultdict(list))
matched_test_cases_by_expert = defaultdict(list)
mismatched_test_cases_by_api = defaultdict(lambda: defaultdict(list))
matched_test_cases_by_api = defaultdict(list)

syntax_errors = defaultdict(list)
skipped_cases = defaultdict(list)

all_experts = ["ShoppingExpert", "AnnouncementExpert", "EmergencyCallExpert", "CallingExpert", "ContactsExpert", "InfoExpert", "FallbackExpert", "BetaExpert", "RecognitionExpert", "EndpointDiscoveryExpert", "VoiceSettingsExpert", "ReservationExpert", "SmartHomeExpert", "ProactiveUpdatesExpert", "None"]

# Rows are for Ground Truth, while Columns are for Predictions
confusion_matrix = [[0 for i in range(len(all_experts))] for j in range(len(all_experts))]

api_expert_map = dict()


def is_last_turn_subsequent_human_turn(turns: Sequence[str]) -> bool:
    return count_human_turns(turns) > 1 and turns[-1].startswith('Human:')


def count_human_turns(turns: Sequence[str]) -> int:
    return sum(1 if turn.startswith('Human:') else 0 for turn in turns)


def is_last_turn_fist_human_turn(turns: Sequence[str]) -> bool:
    if turns[0].startswith('System:'):
        return len(turns) == 2 and turns[1].startswith('Human:')
    else:
        return len(turns) == 1 and turns[0].startswith('Human:')


def get_multiturn_single_turn_ids(samples):
    dialog_id_count = dict()
    for sample in samples:
        dialog_id_count.setdefault(sample["dialog_id"], 0)
        dialog_id_count[sample["dialog_id"]] += 1
    single_turn_ids = [sample for sample in samples if dialog_id_count[sample["dialog_id"]] == 1]
    multi_turn_ids = [sample for sample in samples if dialog_id_count[sample["dialog_id"]] > 1]
    print()
    return single_turn_ids, multi_turn_ids


for input_path in input_paths:
    dataset_name = get_main_name(input_path)
    # print(dataset_name)
    mismatched_test_cases_by_dataset = defaultdict(list)
    single_turn_ids, multi_turn_ids = get_multiturn_single_turn_ids(iter_json_objs(input_path))
    for testcase_index, jobj in enumerate(iter_json_objs(input_path)):
        if 'Response:' in jobj['reference_original']:
            jobj['reference_original'] = jobj['reference_original'].replace('Response:', 'Assistant:')

        reference, reference_type = get_reference(jobj)
        # print(reference)
        # print(reference_type)
        turns = get_conversation_from_post_processed_data(jobj)
        if not turns:
            continue
        if reference_type == ReferenceTypes.Action:
            if turns[-1].lstrip().startswith('Observation:'):
                continue
            api_parse = parse_function_call(reference, separator=';')
            # print(api_parse)
            target_apis = [x[0].strip() for x in api_parse if x and x[0]]
        else:
            target_apis = ["ResponseGeneration"]

        if len(target_apis) == 0:
            syntax_errors[dataset_name].append(testcase_index)
            continue
        assert len(target_apis) == 1, print(f"There are the target apis: {target_apis}")

        # print(target_apis)
        # if "ResponseGeneration" not in target_apis and \
        #    target_apis[0].split(".")[1].lower() not in dataset_name and \
        #    "startemerghelplcall" not in dataset_name and "getemerghelplinelig" not in dataset_name:
        #    print(f"Skipping: {target_apis} for dataset: {dataset_name}")
        #    skipped_cases[dataset_name].append(testcase_index)
        #    continue

        # for api in target_apis:
        #    dataset_name.lower()
        top_level = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning'])
        if top_level:
            dataset_metrics[dataset_name]['trigger'] += 1
            try:
                target_experts = list(
                    flatten_iter(
                        get_experts_for_action_reference(reference, API_GROUNDING_EXPERTS_API_ASSIGNMENT)
                    )
                )
            except Exception as e:
                syntax_errors[dataset_name].append(testcase_index)
                continue

            target_experts = [x for x in target_experts if x != 'FallbackExpert']

            # print("GT Expert:", target_experts)
            if len(target_experts) == 0 or target_experts[0] not in all_experts:
                target_experts = ["FallbackExpert"]
            assert len(target_experts) == 1, print(f"\nProblematic Ground Truth: {target_experts};\nReference: {reference}")

            api_expert_map.setdefault(target_apis[0], set())
            api_expert_map[target_apis[0]].add(target_experts[0])

            top_level_module = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning', 'output', 'moduleName'])
            if top_level_module not in all_experts:
                top_level_module = "FallbackExpert"

            ## Check for multi-turn flags
            # is_multi_turn = jobj["dialog_id"] in multi_turn_ids #is_last_turn_subsequent_human_turn(turns)
            # is_first_turn = jobj["dialog_id"] in single_turn_ids #is_last_turn_fist_human_turn(turns)
            is_multi_turn = is_last_turn_subsequent_human_turn(turns)
            is_first_turn = is_last_turn_fist_human_turn(turns)
            if compute_metrics_for_multi_turn_only and not is_multi_turn:
                continue
            if compute_metrics_for_single_turn_only and not is_first_turn:
                continue
            # print(is_multi_turn)
            ## Fill in the confusion matrix
            gt_ix = all_experts.index(target_experts[0])
            if top_level_module in all_experts:
                predicted_ix = all_experts.index(top_level_module)
            else:
                predicted_ix = len(all_experts) - 1
            # print(gt_ix, predicted_ix)
            # print(len(confusion_matrix))
            # print(max([len(x) for x in confusion_matrix]))
            confusion_matrix[gt_ix][predicted_ix] += 1

            # print("Prediced Expert:", top_level_module)
            mismatch = False
            test_case = {
                'dataset_name': dataset_name,
                'testcase_index': testcase_index,
                'turns': turns,
                'reference': reference,
                'target_experts': target_experts,
                **top_level
            }
            if target_experts:
                for target_expert in target_experts:
                    expert_metrics[target_expert]['total'] += 1
                for target_api in target_apis:
                    api_metrics[target_api]['trigger'] += 1
                expert_metrics[top_level_module]['trigger'] += 1
                # if "DeviceControl.AssistantControlExpert" in top_level_module:
                #    print(top_level_module)
                #    assert False
                if top_level_module in target_experts:
                    dataset_metrics[dataset_name]['tla'] += 1
                    dataset_metrics[dataset_name]['tla_api'] += 1
                    expert_metrics[top_level_module]['tla'] += 1
                    expert_metrics[top_level_module]['tla_api'] += 1
                    for target_api in target_apis:
                        api_metrics[target_api]['tla'] += 1
                        api_metrics[target_api]['tla_api'] += 1

                    for target_expert in target_experts:
                        matched_test_cases_by_expert[target_expert].append(test_case)

                    for target_api in target_apis:
                        matched_test_cases_by_api[target_api].append(test_case)
                else:
                    mismatch = 'undergrab'
                    for target_expert in target_experts:
                        mismatched_test_cases_by_expert[mismatch][target_expert].append(test_case)
                    for target_api in target_apis:
                        mismatched_test_cases_by_api[mismatch][target_api].append(test_case)
                    mismatched_test_cases_by_dataset[mismatch].append(test_case)

                    mismatch = 'overgrab'
                    mismatched_test_cases_by_expert[mismatch][top_level_module].append(test_case)
                    mismatched_test_cases_by_dataset[mismatch].append(test_case)
            else:
                assert False, "code should not come here"
                print("These are target experts")
                print(reference)
                print(target_experts)
                break
                expert_metrics['FallbackExpert']['total'] += 1

                if top_level_module in all_target_experts:
                    mismatch = 'overgrab'
                    mismatched_test_cases_by_expert[mismatch][top_level_module].append(test_case)
                    mismatched_test_cases_by_dataset[mismatch].append(test_case)
                else:
                    dataset_metrics['FallbackExpert']['tla'] += 1
                    dataset_metrics['FallbackExpert']['tla_api'] += 1
                    expert_metrics['FallbackExpert']['tla'] += 1
                    expert_metrics['FallbackExpert']['tla_api'] += 1

                    matched_test_cases_by_expert['FallbackExpert'].append(test_case)

                # eprint_message(mismatched_test_case)
    for mismatch, _mismatched_test_cases_by_dataset in mismatched_test_cases_by_dataset.items():
        write_json_objs(
            _mismatched_test_cases_by_dataset,
            path.join(output_path_mismatched_test_cases_root, 'per_dataset', dataset_name, f'{mismatch}.json'),
        )

for mismatch, _mismatched_test_cases_by_expert in mismatched_test_cases_by_expert.items():
    for expert_name, __mismatched_test_cases_by_expert in _mismatched_test_cases_by_expert.items():
        write_json_objs(
            __mismatched_test_cases_by_expert,
            path.join(output_path_mismatched_test_cases_root, 'per_expert', expert_name, f'{mismatch}.json'),
        )

for expert_name, _matched_test_cases_by_expert in matched_test_cases_by_expert.items():
    write_json_objs(
        _matched_test_cases_by_expert,
        path.join(output_path_matched_test_cases_root, 'per_expert', f'{expert_name}.json'),
    )

for api_name, _matched_test_cases_by_api in matched_test_cases_by_expert.items():
    write_json_objs(
        _matched_test_cases_by_api,
        path.join(output_path_matched_test_cases_root, 'per_api', f'{api_name}.json'),
    )

for mismatch, _mismatched_test_cases_by_api in mismatched_test_cases_by_api.items():
    for api_name, ___mismatched_test_cases_by_api in _mismatched_test_cases_by_api.items():
        write_json_objs(
            ___mismatched_test_cases_by_api,
            path.join(output_path_mismatched_test_cases_root, 'per_api', api_name, f'{mismatch}.json'),
        )

# import pdb; pdb.set_trace()

out_metrics = []
for dataset_name, metrics in dataset_metrics.items():
    out_metrics.append(
        [dataset_name, metrics['trigger'], metrics['tla'], divide_(metrics['tla'], metrics['trigger'], default='N/A')]
    )

save_to_csv(sorted(out_metrics), path.join(output_path_root, 'dataset_metrics.csv'))

out_metrics2 = []
for expert_name, metrics in expert_metrics.items():
    out_metrics2.append(
        [expert_name, metrics['total'], metrics['trigger'], metrics['tla'], divide_(metrics['tla'], metrics['trigger'], default='N/A'), divide_(metrics['tla'], metrics['total'], default='N/A')]
    )
save_to_csv(sorted(out_metrics2), path.join(output_path_root, 'expert_metrics.csv'))

out_metrics3 = []
for api_name, metrics in api_metrics.items():
    out_metrics3.append(
        [api_name, metrics['trigger'], metrics['tla'], divide_(metrics['tla'], metrics['trigger'], default='N/A')]
    )
save_to_csv(sorted(out_metrics3), path.join(output_path_root, 'api_metrics.csv'))

syntax_error_fp = open(path.join(output_path_root, "syntax_errors.json"), "w")
json.dump(syntax_errors, syntax_error_fp)

syntax_error_fp.close()

api_expert_fp = open(path.join(output_path_root, "api_expert_map.json"), "w")
for key, value in api_expert_map.items():
    api_expert_map[key] = list(value)
json.dump(api_expert_map, api_expert_fp)

save_to_csv([[""] + all_experts] + [([all_experts[i]] + row) for i, row in enumerate(confusion_matrix)], path.join(output_path_root, 'confusion_matrix.csv'))

skipped_cases_fp = open(path.join(output_path_root, "skipped_cases.json"), "w")
json.dump(skipped_cases, skipped_cases_fp)
skipped_cases_fp.close()
# print("#### #### Syntax Errors #### ####")
# print(syntax_errors)
# print("#### #### Skipped Cases #### ####")
# print(skipped_cases)
# syntax_errors = defaultdict(list)
# skipped_cases = defaultdict(list)

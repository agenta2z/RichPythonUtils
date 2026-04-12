import glob
from os import path

from rich_python_utils.common_utils import save_to_csv
from rich_python_utils.io_utils.json_io import read_single_line_json_file
from rich_python_utils.path_utils.path_string_operations import get_main_name

input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0626/config_ra_pbo_0626.yaml/ra8bv1_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0626/config_ra_pbo_0626.yaml/ra8bv2_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_non_core_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_multi_turn_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_multi_turn_pre0626_v2_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0626/config_ra_pbo_0626.yaml/ra8bv2_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude3_multi_turn_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0625_haiku/config_claude3_haiku_multi_turn_arbitration_only.yaml/final_results/claude3-haiku_multi-turn_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0625_haiku/config_claude3.yaml/final_results/claude3_haiku-all_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_claude3.yaml/final_results/all_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_haiku_multi_turn_arbitration_only.yaml/final_results/multi-turn_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_haiku_pre0626.yaml/final_results/all_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707.yaml/final_results/claude3_haiku-core_0707'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_0707.yaml/final_results/claude3-core_0707'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35_no_top_level_rules.yaml/claude35-core_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run2'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run3'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run4'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude35_self_learn_shopping.yaml/final_results/claude35-self_learning_shopping_rule'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_no_top_level_dynamic_exemplars.yaml/claude3_haiku-core_0707v2'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707.yaml/claude3_haiku-core_0707v2'

output_path = path.join(input_path_root + '_analysis', 'metrics_summary.csv')

datasets_to_promote = [
    "Exit-Live-V2",
    "Global",
    "ShoppingApi_searchProducts_OPTIMA_WK17_ST",
    "ShoppingApi_searchProducts_OPTIMA_wk12",
    "Weather_1P_V2_down-classified",
    "context_get_user_preference_only",
    "ftb-alarmsapi-editalarm-single-icl-test-dds-test",
    "ftb-alarmsapi-editalarmnextinstance-single-chain-icl-test-dds-test",
    "ftb-alexasubscriptionapi-updatesubscriptionplan-st-icl-test-dds-test",
    "ftb-amazonphotosapi-startphotoframe-single-icl-test-dds-test",
    "ftb-announcement-sendannouncement-multi-icl-test-dds-test",
    "ftb-announcement-sendannouncement-single-icl-test-dds-test",
    "ftb-audio-playx-multi-sft-dds-test",
    "ftb-audio-playx-single-sft-dds-test",
    "ftb-tasksapi-createtodo-single-icl-test-dds-test",
    "ftb-tasksapi-gettasks-single-icl-test-dds-test",
    "ftb-timersapi-createalarm-mixedturn-icl-test-dds-test",
    "ftb-timersapi-getalltimers-single-icl-test-dds-test",
    "ftb-timersapi-shortentimer-single-icl-test-dds-test",
    "ftb-timersapi-snoozetimer-single-icl-test-dds-test",
    "ftb-timerssapi-changetimerduration-single-icl-test-dds-test",
    "ftb-timerssapi-changetimerlabel-single-icl-test-dds-test",
    "ftb-timerssapi-extendtimer-single-icl-test-dds-test",
    "ftb-timerssapi-whatday-single-icl-test-dds-test",
    "ftb-timerssapi-whattime-single-icl-test-dds-test",
    "ftb-transportcontrols-loop-single-icl-test-dds-test",
    "ftb-transportcontrols-next-single-icl-test-dds-test",
    "ftb-transportcontrols-pause-single-icl-test-dds-test",
    "ftb-transportcontrols-previous-single-icl-test-dds-test",
    "ftb-transportcontrols-repeat-single-icl-test-dds-test",
    "ftb-transportcontrols-resume-single-icl-test-dds-test",
    "ftb-transportcontrols-seek-single-icl-test-dds-test",
    "ftb-transportcontrols-shuffle-single-icl-test-dds-test",
    "ftb-transportcontrols-skipbackward-single-icl-test-dds-test",
    "ftb-transportcontrols-skipforward-single-icl-test-dds-test",
    "ftb-transportcontrols-stop-single-icl-test-dds-test",
    "ftb-trustcxinnovation-deletelastconversation-multi-icl-test-dds-test",
    "ftb-trustcxinnovationsapi-getalexaresponse-multi-icl-test-dds-test",
    "ftb-trustcxinnovationsapi-getpreviousutt-multi-icl-test-dds-test",
    "ftb-uinavigation-exit-single-icl-test-dds-test",
    "ftb-uinavigation-goback-single-icl-test-dds-test",
    "ftb-uinavigation-navigatetotarget-single-icl-test-dds-test",
    "ftb-uinavigation-scroll-single-icl-test-dds-test",
    "ftb-uinavigation-select-single-icl-test-dds-test",
    "ftb-uinavigation-showmore-single-icl-test-dds-test",
    "ftb-voicesettingsapi-getcurrentvoice-single-icl-test-dds-test",
    "ftb-voicesettingsapi-listalternativevoice-single-icl-test-dds-test",
    "ftb-voicesettingsapi-setvoicesettings-single-icl-test-dds-test",
    "ftb-voicesettingsapi-setvoicespeakingrate-single-icl-test-dds-test"
]

# datasets_to_promote = []

final_metrics = sorted(glob.glob(path.join(input_path_root, 'metrics', f'*-final_metrics.json')))
expert_metrics = sorted(glob.glob(path.join(input_path_root, 'metrics', f'*-expert_metrics.json')))

for final_metric_file, expert_metric_file in zip(final_metrics, expert_metrics):
    assert get_main_name(final_metric_file).replace('-final_metrics', '') == get_main_name(expert_metric_file).replace('-expert_metrics', '')

output = {}
for final_metric_file, expert_metric_file in zip(final_metrics, expert_metrics):
    dataset_name = get_main_name(final_metric_file).replace('-final_metrics', '')

    final_metrics = read_single_line_json_file(final_metric_file)
    aip = final_metrics['AIP']
    asa = final_metrics['ASA']
    top_level_metrics = read_single_line_json_file(expert_metric_file)

    tla = top_level_metrics['top_level_classification_acc']
    if tla != 0:
        tla2 = top_level_metrics['top_level_api_module_classification_acc']
        tla3 = top_level_metrics['top_level_response_gen_classification_acc']
        if tla3 == 0:
            tla3 = 'N/A'
        output[dataset_name] = [dataset_name, aip, asa, tla, tla2, tla3]

promoted_output = [
    output[dataset_name] for dataset_name in datasets_to_promote
]

other_output = [
    output[dataset_name] for dataset_name in output if dataset_name not in datasets_to_promote
]

other_output = sorted(other_output, key=lambda x: x[0])

save_to_csv(promoted_output + other_output, output_path)

import subprocess

from jinja2 import Template

from rich_python_utils.cli_utils.cmd_execution import list_files
from rich_python_utils.cli_utils.eks_utils.constants import CLI_CMD_EXECUTION
from rich_python_utils.console_utils import hprint_message
from rich_python_utils.path_utils.path_string_operations import get_main_name

chunk_size = 600
input_path_chunked_data = f'/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_600_755653a76de5d65f660825f83d7362e3ed53cc72'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_10000_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1667_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_3000_exclude_0212c2b8bd2a3985c8c79afe2f1f6853fa2190d8'

input_path_chunked_data = f'/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_2000'
input_path_chunked_data = f'/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1000'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_800_exclude_0212c2b8bd2a3985c8c79afe2f1f6853fa2190d8'

input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_500_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'

input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1667_include_427077dc7f35ea0e1ed4da16297f874c0ae095d8'  # +qa
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1000_include_427077dc7f35ea0e1ed4da16297f874c0ae095d8'  # +qa
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_500_include_427077dc7f35ea0e1ed4da16297f874c0ae095d8'  # +qa
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_300_include_e22007e98a0ec8540907a9c0fc5b6dc61c5b066a'

input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0711/test_sets_managed/source_data_chunked/chunk_500'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0711/test_sets_managed/source_data_chunked/chunk_1000'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_1500'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0712/test_sets_managed/source_data_chunked/chunk_1000'

input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0717/wave1_wave2_combined_testsets/source_data_chunked/chunk_1200'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0718/source_data_chunked/chunk_1500'
input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0719/mr_wave1_wave2_train3_0719_2pm_chunk_250'

# input_path_chunked_data = '/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets_managed/source_data_chunked/chunk_dev'

master_pod = 'zgchen-1-pod-r98fp'

input_paths = list_files(
    target=master_pod,
    dir_path=input_path_chunked_data,
    template=CLI_CMD_EXECUTION,
    full_path=True
)

input_paths = sorted(input_paths, key=lambda x: x.rsplit('/', 1)[-1])

CMD_TEMPLATE = """EVAL_CLI_PATH={{ eval_cli_path }}
EVAL_TYPE={{ eval_type }}
EVAL_INDEX={{ eval_index }}
EVAL_ID="$EVAL_TYPE"_$EVAL_INDEX-{{ job_index }}
MODEL_NAME={{ model_name }}
MR_EVAL_VERSION={{ mr_eval_version }}
MR_CONFIG_NAME={{ mr_config_name}}
ICL_CONFIG_NAME={{ icl_config_name }}
JOB_NAME=$MR_EVAL_VERSION-$MODEL_NAME-$EVAL_ID


TEST_WORKSPACE=/data/meta-reasoning-sandbox/code/$MR_EVAL_VERSION
EVAL_CONFIG_DIR=$TEST_WORKSPACE/RAEvaluation/configuration
ICL_CONFIG_FILE=$EVAL_CONFIG_DIR/$ICL_CONFIG_NAME
MR_CONFIG_DIR=$EVAL_CONFIG_DIR/hydra
EVAL_DIR=/data/meta-reasoning-sandbox/evals/$MR_EVAL_VERSION/$MR_CONFIG_NAME/$MODEL_NAME/$EVAL_ID

echo "==== Evaluation Configurations ==="
echo EVAL_CLI_PATH:$EVAL_CLI_PATH
echo EVAL_TYPE:$EVAL_TYPE
echo EVAL_ID:$EVAL_ID
echo MODEL_NAME:$MODEL_NAME
echo MR_EVAL_VERSION:$MR_EVAL_VERSION
echo MR_CONFIG_NAME:$MR_CONFIG_NAME
echo ICL_CONFIG_NAME:$ICL_CONFIG_NAME
echo JOB_NAME:$JOB_NAME
echo "==== Evaluation Configurations ==="

echo "==== Evaluation Paths ==="
echo TEST_WORKSPACE:$TEST_WORKSPACE
echo EVAL_CONFIG_DIR:EVAL_CONFIG_DIR
echo ICL_CONFIG_FILE:$ICL_CONFIG_FILE
echo MR_CONFIG_DIR:$MR_CONFIG_DIR
echo EVAL_DIR:EVAL_DIR
echo "==== Evaluation Paths ==="

python3 $EVAL_CLI_PATH/src/auto_evaluation/eval_cli.py eval \\
--model-ids $MODEL_NAME \\
--eval-metric MetaReason \\
--dataset-names {{ dataset_names }} \\
--dataset-paths {{ dataset_paths}} \\
--icl-config $ICL_CONFIG_FILE \\
--eval-dir $EVAL_DIR \\
--include-prev-turns-observation \\
--job-name $JOB_NAME \\
--testing-workspace $TEST_WORKSPACE \\
--client-config-dir $MR_CONFIG_DIR \\
--client-config-name $MR_CONFIG_NAME
"""

tempalte = Template(CMD_TEMPLATE)

model_name = 'claude3'
# mr_config_name = 'config_claude_oracle_top_level_api_planning_icl.yaml'
# mr_config_name = 'config_claude_no_top_level_api_planning_icl.yaml'

eval_cli_path = "/home/zgchen/RAEvaluation_latest/src/RAEvaluation"
eval_type = 'non_core_0626_claude35'
eval_type = 'core_0627_no_rules_claude35'
eval_type = 'core_0627_claude3'
eval_type = 'core_0627_no_rules_claude3'
eval_type = 'non_core_0626_claude3'
eval_type = 'core_0626_c3'
eval_type = 'all_pre0626_c3'
eval_type = 'all_pre0626_c3h'
eval_type = 'all_0707_c3h'
eval_type = 'dev_0626_c3_run8'
eval_type = 'non_core_0707_c3h'
eval_type = 'sl_sp_0707_c35'
eval_type = 'core_0707_c3h_ntlde_r2'
eval_type = 'core_0707v2_c3h'
eval_type = 'core_0707v2nde_c3h'
eval_type = 'core_0707v2ntnde_c3h'
eval_type = 'prod0711_c3h'
eval_type = 'prod0711_nf_c3h'
eval_type = 'prod0712iq_c3h'
eval_type = 'prod0707_c3h'
eval_type = 'prod0712iq_c3h_r14'
eval_type = 'prod0717_c3h_r3'
eval_type = 'prod0719_c3h_sh'

eval_index = 0
mr_eval_version = 'e2e_0622'
mr_eval_version = 'e2e_0623'
mr_eval_version = 'e2e_0624'
mr_eval_version = 'e2e_0621'
mr_eval_version = 'e2e_0625_haiku'
mr_eval_version = 'e2e_0627'
mr_eval_version = 'e2e_0625'
mr_eval_version = 'e2e_0707_haiku_dev'
mr_eval_version = 'e2e_0707_dev'
# mr_eval_version = 'e2e_0710_dev2'
mr_eval_version = 'e2e_0710_dev3'  # run 3
mr_eval_version = 'e2e_0710_dev'  # run 3
mr_eval_version = 'e2e_0709_dev'  # run 8
mr_eval_version = 'e2e_0707_haiku_dev2'
mr_eval_version = 'e2e_0707_haiku_dev3'
mr_eval_version = 'e2e_0707_haiku_dev5'
mr_eval_version = 'e2e_0707_haiku_dev4'
mr_eval_version = 'e2e_0707_haiku_dev7'
mr_eval_version = 'e2e_0707_haiku_dev6'

icl_config_name = 'icl_baseline_pcov3_250apis.yaml'
icl_config_name = 'icl_baseline_pcov3_250apis_mr.yaml'

mr_configs = [
    'config_claude_oracle_top_level_api_planning_verbose_xml_bk.yaml',
    'config_claude_no_top_level_api_planning_verbose_xml_bk.yaml',
    'config_claude_oracle_top_level.yaml',
    'config_claude_no_top_level.yaml'
]

mr_configs = [
    'config_claude35.yaml'
]

mr_configs = [
    'config_claude3_no_top_level_rules.yaml'
]

mr_configs = [
    'config_claude3_pre0625.yaml'
]

mr_configs = [
    'config_claude3_pre0626.yaml'
]

mr_configs = [
    'config_claude3.yaml'
]

mr_configs = ['config_claude35_self_learn_shopping.yaml']

mr_configs = [
    'config_claude3_haiku_no_top_level_dynamic_exemplars.yaml'
]

mr_configs = [
    'config_claude3_haiku_0707.yaml'
]

mr_configs = ['config_claude3_haiku_0707_no_top_level_rules.yaml']

mr_configs = ['config_claude3_haiku_0707_no_top_level_dynamic_exemplars.yaml']

mr_configs = ['config_claude3_haiku_0707_slsp.yaml']

mr_configs = ['config_claude3_haiku_0707_slsp_ndexp.yaml']

mr_configs = ['config_claude3_haiku_0707_no_top_level_rules_and_dynamic_exemplars.yaml']

mr_configs = ['config_claude3_haiku_0711_prod.yaml']

mr_configs = ['config_claude3_haiku_0711_prod_with_fallback.yaml']

mr_configs = ['config_claude3_haiku_0711_prod_with_fallback_infoqa.yaml']

mr_configs = ['config_claude3_haiku_0716_prod_with_fallback_and_added_tags.yaml']

mr_configs = ['config_claude3_haiku_wave2_prod_no_fallback_0716.yaml']

mr_configs = ['config_claude3_haiku_wave2_prod_with_fallback_0716.yaml']

mr_configs = ['config_claude3_haiku_wave2_prod_with_fallback_0716_v2.yaml']

mr_configs = ['config_claude3_haiku_wave2_prod_with_fallback_0716_v3.yaml']

mr_configs = ['config_claude3_haiku_wave2_prod_no_fallback_0716_v2.yaml']

mr_configs = ['config_claude3_haiku_wave2_prod_no_fallback_0716_v3.yaml']

from time import sleep

for config_index, mr_config_name in enumerate(mr_configs):
    for job_index, input_path in enumerate(input_paths[1:]):
        dataset_paths = [input_path]
        dataset_names = [get_main_name(dataset_path) for dataset_path in dataset_paths]

        template_feed = {
            'eval_cli_path': eval_cli_path,
            'eval_type': eval_type,
            'eval_index': f'{eval_index}-{config_index}',
            'job_index': job_index,
            'model_name': model_name,
            'mr_eval_version': mr_eval_version,
            'mr_config_name': mr_config_name,
            'icl_config_name': icl_config_name,
            'dataset_paths': ' '.join(dataset_paths),
            'dataset_names': ' '.join(dataset_names)
        }

        cmd = Template(CMD_TEMPLATE).render(**template_feed)
        execution_result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        hprint_message(
            {
                **template_feed,
                'cmd': cmd,
                'stdout': execution_result.stdout,
                'stderr': execution_result.stderr
            }
        )

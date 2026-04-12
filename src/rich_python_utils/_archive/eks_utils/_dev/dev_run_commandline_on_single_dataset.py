import subprocess

from jinja2 import Template

from rich_python_utils.path_utils.path_string_operations import get_main_name

job_index = 0
model_name = 'v18-OP1'
mr_config_name = 'config-top_level_more_structure-api_planning_verbose.yaml'
dataset_paths = ['/data/meta-reasoning-sandbox/data/evaluation_datasets/0501/test_sets/ShoppingApi_searchProducts_OPTIMA_wk12.jsonl']

eval_cli_path = "/home/zgchen/RAEvaluation_latest/src/RAEvaluation"
eval_type = 'e2e'
mr_eval_version = 'e2e_0610'
icl_config_name = 'icl_baseline_pcov3_250apis.yaml'
dataset_names = [get_main_name(dataset_path) for dataset_path in dataset_paths]


template_feed = {
    'eval_cli_path': eval_cli_path,
    'eval_type': eval_type,
    'job_index': job_index,
    'model_name': model_name,
    'mr_eval_version': mr_eval_version,
    'mr_config_name': mr_config_name,
    'icl_config_name': icl_config_name,
    'dataset_paths': ' '.join(dataset_paths),
    'dataset_names': ' '.join(dataset_names)
}

CMD_TEMPLATE = """EVAL_CLI_PATH={{ eval_cli_path }}
EVAL_TYPE={{ eval_type }}
EVAL_ID="$EVAL_TYPE"_$(date +%s)-{{ job_index }}
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

cmd = Template(CMD_TEMPLATE).render(**template_feed)

print(cmd)

execution_result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
print(execution_result.stdout)
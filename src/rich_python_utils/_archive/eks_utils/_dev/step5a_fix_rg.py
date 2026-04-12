import glob

from rich_python_utils.common_utils import get_value_by_path
from rich_python_utils.io_utils.json_io import iter_json_objs, write_json_objs

input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_non_core_by_dataset'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0625_haiku/config_claude3.yaml/final_results/claude3-haiku_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_claude3.yaml/final_results/all_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_haiku_multi_turn_arbitration_only.yaml/final_results/multi-turn_0626'
input_path_root = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35_no_top_level_rules.yaml/claude35-core_0626'

input_paths = glob.glob(input_path_root + '/*/post-process/*.json*')

for input_path in input_paths:
    out = []
    for jobj in iter_json_objs(input_path):
        top_level = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning'])
        if top_level:

            top_level_module = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning', 'output', 'moduleName'])
            if 'Response:' in top_level_module:
                response = top_level_module.split('Response:', maxsplit=1)[1].strip()
                jobj['reasoning_outputs'][0]['TopLevelPlanning']['output'] = {
                    'task': response,
                    'moduleType': 'ResponseGeneration',
                    'moduleName': 'ResponseGeneration'
                }
                if len(list(jobj['reasoning_outputs'][1].keys())) == 1 and list(jobj['reasoning_outputs'][1].keys())[0] == 'APIPlanning':
                    jobj['reasoning_outputs'] = jobj['reasoning_outputs'][:1]
                jobj['context'][0]['TopLevelPlanning']['output'] = jobj['reasoning_outputs'][0]['TopLevelPlanning']['output']
                jobj['expert_level_info']['top_level_info']['output'] = jobj['reasoning_outputs'][0]['TopLevelPlanning']['output']
                jobj['expert_level_info']['response_generation_info'] = {
                    "response_generation_executed": True,
                    "output": response,
                }
                if len(list(jobj['context'][1].keys())) == 1 and list(jobj['context'][1].keys())[0] == 'APIPlanning':
                    jobj['context'] = jobj['context'][:1]
                jobj['response_original'] = 'Assistant: ' + response
                jobj['response'] = response
            else:
                jobj['expert_level_info']['response_generation_info'] = {
                    "response_generation_executed": False,
                    "output": "",
                }

        out.append(jobj)
    write_json_objs(out, input_path)

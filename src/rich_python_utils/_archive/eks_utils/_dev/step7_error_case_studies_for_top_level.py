from camel_llm_prompter._dev.top_level.evaluation.planning.utils import get_experts_for_action_reference
from camel_llm_prompter._dev.top_level.utils.config_utils import get_config_path
from camel_llm_prompter._dev.top_level.utils.data_utils import get_reference, get_conversation_from_post_processed_data, ReferenceTypes
from rich_python_utils.common_utils import get_value_by_path
from rich_python_utils.common_utils.iter_helper import flatten_iter
from rich_python_utils.console_utils import hprint_message
from rich_python_utils.io_utils.json_io import iter_json_objs, iter_all_json_objs_from_all_sub_dirs, write_json_objs
import json

API_GROUNDING_EXPERTS_API_ASSIGNMENT = json.load(
    open(get_config_path('top_level/planning/v1/expert_info.json'))
)

input_path = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3_by_dataset/ShoppingApi_searchProducts_OPTIMA_WK17_ST/post-process/ShoppingApi_searchProducts_OPTIMA_WK17_ST.jsonl'
input_path = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707/ShoppingApi_searchProducts_OPTIMA_wk12/post-process/ShoppingApi_searchProducts_OPTIMA_wk12.jsonl'
input_path = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run2/ShoppingApi_searchProducts_OPTIMA_wk12/post-process/ShoppingApi_searchProducts_OPTIMA_wk12.jsonl'
input_path = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run3/ShoppingApi_searchProducts_OPTIMA_wk12/post-process/ShoppingApi_searchProducts_OPTIMA_wk12.jsonl'

for jobj in iter_json_objs(input_path):
    reference, reference_type = get_reference(jobj)
    if reference_type == ReferenceTypes.Action:
        top_level = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning'])
        if top_level:
            turns = get_conversation_from_post_processed_data(jobj)
            target_experts = list(
                flatten_iter(
                    get_experts_for_action_reference(reference, API_GROUNDING_EXPERTS_API_ASSIGNMENT)
                )
            )
            top_level_module =  get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning', 'output', 'moduleName'])
            if top_level_module not in target_experts:
                hprint_message(
                    'turns', turns,
                    'reference', reference,
                    'target_experts', target_experts
                )

                hprint_message(
                    top_level
                )

                import pdb; pdb.set_trace()

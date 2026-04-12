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

input_path = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_by_dataset_dev/ShoppingApi_searchProducts_OPTIMA_WK17_ST/post-process/ShoppingApi_searchProducts_OPTIMA_WK17_ST.jsonl'

for jobj in iter_json_objs(input_path):
    reference, reference_type = get_reference(jobj)
    if reference_type == ReferenceTypes.DirectResponse:
        top_level = get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning'])
        if top_level:
            top_level_module =  get_value_by_path(jobj, ['reasoning_outputs', 0, 'TopLevelPlanning', 'output', 'moduleName'])
            if not top_level_module.startswith('Response:'):
                hprint_message(
                    'reference', reference,
                    'top_level_module', top_level_module
                )

                hprint_message(
                    top_level
                )

                import pdb; pdb.set_trace()

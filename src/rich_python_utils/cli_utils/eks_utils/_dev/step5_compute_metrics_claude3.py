import glob
from os import path
from pprint import pprint

import json
import logging
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional

import hydra
from hydra.utils import instantiate
from omegaconf import DictConfig, OmegaConf

from camel_llm_evaluation.constants import DEFAULT_EVAL_STEPS, EvalSteps
from camel_llm_evaluation.metrics.score_base import BaseScore
from camel_llm_evaluation.utils import (
    download_file_if_on_s3,
    get_dataset_name,
    get_output_path,
)
from datasets import load_dataset

from camel_llm_prompter.adapter.meta_reasoning_orchestrator import MetaReasoningOrchestrator

input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0621_claude3/config_claude3.yaml/claude3_by_dataset'
mr_version = 'e2e_0621_claude3'

input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_by_dataset'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_non_core_by_dataset'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_multi_turn_by_dataset'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_multi_turn_pre0626_v2_by_dataset'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude3_multi_turn_by_dataset'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0626/config_ra_pbo_0626.yaml/ra8bv2/restored_results/v1p9-gg-ppm10-r2_0626'
input_path_eval_dir =  '/data/meta-reasoning-sandbox/evals/e2e_0625_haiku/config_claude3.yaml/final_results/claude3-haiku_0626'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0621/config_claude3.yaml/final_results/all_0626'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_haiku_multi_turn_arbitration_only.yaml/final_results/multi-turn_0626'
input_path_eval_dir =  '/data/meta-reasoning-sandbox/evals/e2e_0627/config_claude3_haiku_pre0626.yaml/final_results/all_0626'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707.yaml/final_results/claude3_haiku-core_0707'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_0707.yaml/final_results/claude3-core_0707'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35_no_top_level_rules.yaml/claude35-core_0626'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run2'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run3'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude35_self_learn_shopping.yaml/final_results/claude35-self_learning_shopping_rule'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_mt_claude3_haiku.yaml/final_results/claude3-haiku_multi_turn_0707_run4'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707.yaml/claude3_haiku-core_0707v2'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_no_top_level_rules.yaml/claude3_haiku-core_0707v2'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_no_top_level_dynamic_exemplars.yaml/claude3_haiku-core_0707v2'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_slsp.yaml/claude3_haiku-core_0707v2'
input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0707/config_claude3_haiku_0707_slsp_ndexp.yaml/claude3_haiku-core_0707v2'
# input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0622/config_claude35.yaml/claude35_by_dataset_dev'
mr_version = 'e2e_0622'

config_name = 'config_claude35.yaml'


# input_path_eval_dir = '/data/meta-reasoning-sandbox/evals/e2e_0626/config_ra_pbo_0626.yaml/ra8bv2_by_dataset'
# mr_version = 'e2e_0626'
# config_name = 'config_ra_pbo_0626.yaml'

test_workspace = path.join('/data/meta-reasoning-sandbox/code')
config_path = path.join(test_workspace, mr_version, 'RAEvaluation/configuration/hydra')




logger = logging.getLogger(__name__)


def download_input_files(config: DictConfig, tmp_workspace: str):
    """Download data files if on S3, and update the config"""
    tmp_workspace_path = Path(tmp_workspace)

    if config.get("system_prompt_file", None):
        logger.info(f"Downloading system prompt file if on s3...")
        local_sys_prompt_path = download_file_if_on_s3(
            config["system_prompt_file"], tmp_workspace_path / "system_prompt"
        )
        OmegaConf.update(config, "system_prompt_file", local_sys_prompt_path)
        logger.info(f"System prompt file downloaded at {local_sys_prompt_path}")

    if config.get("icl_prompt_config_file", None):
        logger.info(f"Downloading ICL prompt config file if on s3...")
        local_icl_prompt_path = download_file_if_on_s3(
            config["icl_prompt_config_file"], tmp_workspace_path / "icl_config"
        )
        OmegaConf.update(config, "icl_prompt_config_file", local_icl_prompt_path)
        logger.info(f"ICL prompt config file downloaded at {local_icl_prompt_path}")

    logger.info(f"Downloading dataset...")

    # Not needed for now, none of the files are in S3
    # local_dataset_path = download_file_if_on_s3(
    #     config.get("dataset_path"), tmp_workspace_path / "datasets" / os.path.basename(config.get("dataset_path"))
    # )
    # OmegaConf.update(config, "dataset_path", local_dataset_path)
    # logger.info(f"Dataset downloaded at {local_dataset_path}")

    return config


def run_metrics_generation(
        input_data_files: List[str],
        output_dir: str,
        final_metric_calculator: BaseScore,
        component_metric_calculator: Optional[BaseScore],
        expert_metric_calculator: Optional[BaseScore],
        pco_metric_calculator: Optional[BaseScore]
):
    """Run metrics generation on the input data files"""
    for input_data_file in input_data_files:
        logger.info(f"Running metric generation for {input_data_file}")
        input_ds = load_dataset("text", data_files=input_data_file)["train"]
        dataset_name = get_dataset_name(input_data_file)
        metrics_dir = get_output_path(output_dir + "/" + EvalSteps.METRICS)

        if component_metric_calculator is not None:
            component_metrics = component_metric_calculator.compute_metrics(input_ds)
            logger.info(f"component_metrics: {component_metrics}")
            with open(
                    os.path.join(metrics_dir, dataset_name + "-component_metrics.json"), "w"
            ) as f:
                f.write(json.dumps(component_metrics))

        if expert_metric_calculator is not None:
            expert_metrics = expert_metric_calculator.compute_metrics(input_ds)
            logger.info(f"expert_metrics: {expert_metrics}")
            with open(
                    os.path.join(metrics_dir, dataset_name + "-expert_metrics.json"), "w"
            ) as f:
                f.write(json.dumps(expert_metrics))

        metrics_generation_start_time = time.time()
        try:
            final_metrics = final_metric_calculator.compute_metrics(input_ds)
        except Exception as e:
            raise e
        with open(os.path.join(metrics_dir, dataset_name + "-final_metrics.txt"), "w") as f:
            for i in final_metrics[1]:
                f.write(str(i) + "\n")
        logger.info(f"final_metrics: {final_metrics[1]}")
        try:
            with open(os.path.join(metrics_dir, dataset_name + "-final_metrics.json"), "w") as f:
                f.write(json.dumps(final_metrics[2], default=str))
            logger.info(f"final_metrics: {final_metrics[2]}")
            metrics_generation_completion_time = time.time()
            logger.info(
                f"Metrics generation execution duration: "
                f"{metrics_generation_completion_time - metrics_generation_start_time} seconds."
            )
        except Exception as e:
            logger.error(f"Error while generating final_metrics.json file: {e}")

        if pco_metric_calculator is not None:
            pco_metrics = pco_metric_calculator.compute_metrics(input_ds)
            with open(os.path.join(metrics_dir, dataset_name + "-pco_metrics.json"), "w") as f:
                f.write(json.dumps(pco_metrics))

            logger.info(f"pco metrics: {pco_metrics}")


def run_meta_evaluation(config: dict, eval_dir: str):
    import glob
    from os import path
    from pprint import pprint
    post_processed_output_paths = glob.glob(path.join(eval_dir, '*', 'post-process', '*.json*'))
    pprint(post_processed_output_paths)

    # initialize the metric generation
    final_metric_calculator = instantiate(config["benchmark"]["final-metric"])
    if "component-metric" in config["benchmark"]:
        component_metric_calculator = instantiate(config["benchmark"]["component-metric"])
    else:
        component_metric_calculator = None

    if "expert-metric" in config["benchmark"]:
        expert_metric_calculator = instantiate(config["benchmark"]["expert-metric"])
    else:
        expert_metric_calculator = None

    # set pco metric to None
    # TODO: we need to bring pco metric generation into parity here too
    pco_metric_calculator = None
    run_metrics_generation(
        post_processed_output_paths,
        eval_dir,
        final_metric_calculator,
        component_metric_calculator,
        expert_metric_calculator,
        pco_metric_calculator,
    )


@hydra.main(version_base=None, config_path=config_path, config_name=config_name)
def main_fn(cfg: DictConfig):
    with TemporaryDirectory() as tmp_workspace:
        logger.info(f"Creating tmp workspace: {tmp_workspace}")
        cfg = download_input_files(cfg, tmp_workspace)

        config = OmegaConf.to_container(cfg, resolve=True)
        logger.info(f"Configuration: {config}")

        eval_dir = input_path_eval_dir

        if not config.get("steps"):
            # Run all steps if not specified
            config["steps"] = DEFAULT_EVAL_STEPS

        logger.info("Running steps: %s", config["steps"])

        run_meta_evaluation(config, eval_dir)

        # this is to signal to ft server to shutdown
        if Path(config["shared_dir"]).exists():
            logger.info("Creating output file to signal ft server shutdown")
            (Path(config["shared_dir"]) / "finished").touch()


if __name__ == "__main__":
    main_fn()

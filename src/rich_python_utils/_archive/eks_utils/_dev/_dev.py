import subprocess
import tempfile
from typing import Mapping, Set, Tuple, Optional, List, Union, Sequence
from jinja2 import Template
from jinja2 import Environment, meta
from os import path
import os

from rich_python_utils.io_utils.text_io import read_all_text, write_all_text

CMD_EKS_GREEN_PERMISSION = """ada credentials update --provider=conduit --role={{role}} --account={{account}} --once 
aws eks --region {{cluster_region}} update-kubeconfig --name {{cluster_name}}"""

CMD_EKS_LAUNCH = """kubectl apply -f {{config_path}}"""

CONFIG_EKS_GREEN_LAUNCH = """apiVersion: batch/v1
kind: Job
metadata:
  name: {{instance_name}}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 500
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        node.kubernetes.io/instance-type: {{instance_type}}
      containers:
      - image: 934169004475.dkr.ecr.us-west-2.amazonaws.com/ra_llm_eval:latest
        imagePullPolicy: Always
        resources:
          limits:
            aws.amazon.com/neuron: "16"
            vpc.amazonaws.com/efa: "8"
          requests:
            aws.amazon.com/neuron: "16"
            vpc.amazonaws.com/efa: "8"
        name: launcher
        securityContext:
          privileged: true
        command: ["/bin/bash"]
        args:
        - "-c"
        - sleep infinity
        volumeMounts:
        - mountPath: /checkpoints
          name: checkpoints-fsx
        - mountPath: /code
          name: code-fsx
        - mountPath: /data
          name: data-fsx
      securityContext: {}
      volumes:
      - name: checkpoints-fsx
        persistentVolumeClaim:
          claimName: fsx-claim-general-c3
      - name: code-fsx
        persistentVolumeClaim:
          claimName: fsx-claim-general-c1
      - name: data-fsx
        persistentVolumeClaim:
          claimName: fsx-claim-general-c2"""


def read_all_text_(file_path_or_content: str, encoding: Optional[str] = None) -> str:
    if file_path_or_content:
        if path.isfile(file_path_or_content):
            return read_all_text(file_path_or_content, encoding)
        else:
            return file_path_or_content


def get_tmp_file_path() -> str:
    temp_fd, temp_path = tempfile.mkstemp()
    os.close(temp_fd)
    return temp_path


def get_template_arg_names(*template: str) -> Set:
    out = None
    for _template in template:
        if _template:
            _template = read_all_text_(_template)
            env = Environment()
            ast = env.parse(_template)
            args = meta.find_undeclared_variables(ast)
            if out is None:
                out = args
            else:
                out |= args
    return out


def get_cmd_and_config_path(cmd_template: str, config_template: str = None, config_path_arg_name='config_path', **kwargs) -> Tuple[str, Optional[str]]:
    config_path = None
    if config_template is not None:
        config = Template(read_all_text_(config_template)).render(**kwargs)
        if config_path_arg_name in kwargs:
            config_path = kwargs[config_path_arg_name]
        else:
            config_path = get_tmp_file_path()
            kwargs = {**kwargs, config_path_arg_name: config_path}

        write_all_text(
            file_path=config_path,
            text=config
        )
    return Template(cmd_template).render(**kwargs), config_path


def sub_dict(d: Mapping, sub_keys):
    return {key: d[key] for key in sub_keys if key in d}


def execute_cmd_with_config(cmd_template: str, config_template: str, arg_dict: Mapping = None, **kwargs):
    if cmd_template:
        config_template = read_all_text_(config_template)
        arg_names = get_template_arg_names(cmd_template, config_template)
        if arg_dict:
            for arg_name in arg_dict:
                if arg_name not in arg_names:
                    raise ValueError(f"launch arg '{arg_name}' cannot be found in the cmd template `{cmd_template}` and the config template: `{config_template}`")
        arg_dict_merged = {**arg_dict, **sub_dict(kwargs, arg_names)}
        cmd = Template(cmd_template).render(**arg_dict_merged)
        return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def execute_cmds_with_config(*cmds: Union[str, Tuple[str, str], Tuple[str, str, Mapping]], **kwargs):
    for cmd in cmds:
        if not cmd:
            continue

        _cmd = None
        if isinstance(cmd, str):
            _cmd = cmd
            _config_template = _arg_dict = None
        if isinstance(cmd, Sequence):
            if len(cmd) == 2:
                _cmd, _config_template = cmd
            elif len(cmd) == 3:
                _cmd, _config_template, _arg_dict = cmd
        if _cmd is None:
            raise ValueError(f"`{cmd}` cannot be recognized as a command")

        execute_cmd_with_config(_cmd, _config_template, _arg_dict, **kwargs)


def create_service(
        launch_cmd_template: str,
        launch_config_template: str = None,
        launch_args: Mapping = None,
        permission_cmd_template: str = None,
        permission_config_template: str = None,
        permission_args: Mapping = None,
        **kwargs
):
    execute_cmds_with_config(
        (permission_cmd_template, permission_config_template, permission_args),
        (launch_cmd_template, launch_config_template, launch_args),
        **kwargs
    )


create_service(
    launch_cmd_template=CMD_EKS_LAUNCH,
    launch_config_template=CONFIG_EKS_GREEN_LAUNCH,
    permission_cmd_template=CMD_EKS_GREEN_PERMISSION,
    role='scientist',
    account='107280343912',
    cluster_region='us-east-2',
    cluster_name='Cluster9EE0221C-30a6520360a94597a65ca7d0796a9f29',
    instance_type='trn1.32xlarge',
    instance_name='zgchen-pod-20'
)

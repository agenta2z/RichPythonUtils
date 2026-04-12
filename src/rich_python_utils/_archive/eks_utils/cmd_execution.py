from functools import partial

from rich_python_utils.cli_utils.cmd_execution import execute_cmd as _execute_cmd
from rich_python_utils.cli_utils.cmd_execution import dir_size as _dir_size
from rich_python_utils.cli_utils.cmd_execution import listdir as _listdir
from rich_python_utils.cli_utils.cmd_execution import list_files as _list_files
from rich_python_utils.cli_utils.cmd_execution import list_subdirs as _list_subdirs
from rich_python_utils.cli_utils.eks_utils.constants import CLI_CMD_EXECUTION

execute_cmd = partial(_execute_cmd, template=CLI_CMD_EXECUTION)
dir_size = partial(_dir_size, template=CLI_CMD_EXECUTION)
listdir = partial(_listdir, template=CLI_CMD_EXECUTION)
list_files = partial(_list_files, template=CLI_CMD_EXECUTION)
list_subdirs = partial(_list_subdirs, template=CLI_CMD_EXECUTION)

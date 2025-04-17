#!/usr/bin/env python

'''
This is a SLURM class to be used with pbs4py and was modified from pbs.py

Written/Copied by Matt Opgenorth
'''
import os
from typing import List, Union

from pbs4py.launcher_base import Launcher


class SLURM(Launcher):
    def __init__(
        self,
        queue_name: str = "normal",
        ncpus_per_node: int = 64,
        ngpus_per_node: int = 0,
        queue_node_limit: int = 30,
        time: int = 24,
        mem: str = None,
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
    ):
        """
        | A class for creating and running slurm jobs.
        | Defaults not set during instantiation can be adjusted by directly modifying attributes.

        Parameters
        ----------
        queue_name:
            Queue name which goes on the "#SBATCH --partition {queue_name}" line of the slurm header
        ncpus_per_node:
            Number of CPU cores per node
        ngpus_per_node:
            Number of GPUs per node
        queue_node_limit:
            Maximum number of nodes allowed in this queue
        time:
            The requested job walltime in hours
        mem:
            The requested memory size. String to allow specifying in G, MB, etc.
        profile_file:
            The file setting the environment to source inside the SLURM job. Set to
            '' if you do not wish to source a file.
        requested_number_of_nodes:
            The number of compute nodes to request
        """
        super().__init__(ncpus_per_node, ngpus_per_node, queue_node_limit,
                         time, profile_filename, requested_number_of_nodes)

        #: The name of the queue which goes on the ``#SBATCH --partition {queue_name}``
        #: line of the slurm header
        self.queue_name: str = queue_name

        #: The account for the account entry of the slurm header if necessary.
        #: The associated SLURM header line is ``#SBATCH --account={account}``
        self.account: str = None

        #: Requested memory size on the select line. Need to include units in the str.
        #: The associated SLURM header line is ``#SBATCH --mem={mem}``
        self.mem: Union[str, None] = mem

        #: Index range for SLURM array of jobs
        #: The associated SLURM header line is ``#SBATCH --array={array_range}``
        self.array_range: Union[str, None] = None

        #: ``sbatch --mail-type`` mail options. BEGIN, END, FAIL
        self.mail_options: str = None

        #: ``sbatch --mail-user`` mail list. Who to email when mail_options are triggered
        self.mail_list: Union[str, None] = None

        #: Type of dependency if dependency active.
        #: Default is 'afterok' which only launches the new job if the previous one was successful.
        self.dependency_type: str = "afterok"

        self.mpiexec: str = "mpiexec"
        self.ranks_per_node_flag: str = None

        self.workdir_env_variable = "$SLURM_SUBMIT_DIR"
        self.batch_file_extension = "slurm"
        self.mpiprocs_per_node = None
        self.requested_number_of_nodes = requested_number_of_nodes

        #: nodelist
        self.nodelist: str = None

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        header_lines = [
            self._create_hashbang(),
            self._create_job_line_of_header(job_name),
            self._create_queue_line_of_header(),
            self._create_nodes_line_of_header(),
            self._create_tasks_per_node_line_of_header(),
            self._create_walltime_line_of_header(),
            self._create_log_name_line_of_header(job_name),
            self._create_header_line_to_error_output(job_name),
            self._create_header_line_to_set_that_job_is_not_rerunnable(),
        ]
        return header_lines

    def _create_job_line_of_header(self, job_name: str) -> str:
        return f"#SBATCH --job-name={job_name}"

    def _create_queue_line_of_header(self) -> str:
        return f"#SBATCH --partition={self.queue_name}"

    def _create_nodes_line_of_header(self) -> str:
        return f'#SBATCH --nodes={self.requested_number_of_nodes}'

    def _create_tasks_per_node_line_of_header(self) -> str:
        return f'#SBATCH --ntasks-per-node={self.ncpus_per_node}'

    def _create_walltime_line_of_header(self) -> str:
        return f"#SBATCH --time={self.time}:00:00"

    def _create_log_name_line_of_header(self, job_name: str) -> str:
        return f"#SBATCH --output=qlog_{job_name}"

    def _create_header_line_to_error_output(self, job_name: str):
        return f"#SBATCH --error=err_{job_name}"

    def _create_header_line_to_set_that_job_is_not_rerunnable(self) -> str:
        return "#SBATCH --no-requeue"

    def _create_list_of_optional_header_lines(self, dependency):
        header_lines = []
        header_lines.extend(self._create_account_header_line())
        header_lines.extend(self._create_array_range_header_line())
        header_lines.extend(self._create_mail_options_header_lines())
        header_lines.extend(self._create_job_dependencies_header_line(dependency))
        header_lines.extend(self._create_nodelist_header_line())
        return header_lines

    def _create_account_header_line(self) -> List[str]:
        if self.account is not None:
            return [f"#SBATCH --account={self.account}"]
        else:
            return []

    def _create_array_range_header_line(self) -> List[str]:
        if self.array_range is not None:
            return [f"#SBATCH --array={self.array_range}"]
        else:
            return []

    def _create_mail_options_header_lines(self) -> List[str]:
        header_lines = []
        if self.mail_options is not None:
            header_lines.append(f"#SBATCH --mail-type={self.mail_options}")
        if self.mail_list is not None:
            header_lines.append(f"#SBATCH --mail-user={self.mail_list}")
        return header_lines

    def _create_job_dependencies_header_line(self, dependency) -> List[str]:
        if dependency is not None:
            return [f"#SBATCH --dependency={self.dependency_type}:{dependency}"]
        else:
            return []

    def _create_nodelist_header_line(self) -> List[str]:
        if self.nodelist is not None:
            return [f"#SBATCH --nodelist={self.nodelist}"]
        else:
            return []

    def _run_job(self, job_filename: str, blocking: bool, print_command_output=True) -> str:
        options = ""
        if blocking:
            options += "-W"
        command_output = os.popen(f"sbatch {options} {job_filename}").read().strip()
        if print_command_output:
            print(command_output)
        return command_output

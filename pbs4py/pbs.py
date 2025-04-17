#!/usr/bin/env python
import os
import subprocess
from typing import List, Union
import numpy as np

from pbs4py.launcher_base import Launcher


class PBS(Launcher):
    def __init__(
        self,
        queue_name: str = "K4-route",
        ncpus_per_node: int = 40,
        ngpus_per_node: int = 0,
        queue_node_limit: int = 10,
        time: int = 72,
        mem: str = None,
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
    ):
        """
        | A class for creating and running pbs jobs. Default queue properties are for K4.
        | Defaults not set during instantiation can be adjusted by directly modifying attributes.

        Parameters
        ----------
        queue_name:
            Queue name which goes on the "#PBS -N {name}" line of the pbs header
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
            The file setting the environment to source inside the PBS job. Set to
            '' if you do not wish to source a file.
        requested_number_of_nodes:
            The number of compute nodes to request
        """
        super().__init__(ncpus_per_node, ngpus_per_node, queue_node_limit,
                         time, profile_filename, requested_number_of_nodes)

        #: The name of the queue which goes on the ``#PBS -N {queue_name}``
        #: line of the pbs header
        self.queue_name: str = queue_name

        #: The processor model if it needs to be specified.
        #: The associated PBS header line is ``#PBS -l select=#:ncpus=#:mpiprocs=#:model={model}``
        #: If left as `None`, the ``:model={mode}`` will not be added to the header line
        self.model: Union[str, None] = None

        #: The group for the group_list entry of the pbs header if necessary.
        #: The associated PBS header line is ``#PBS -W group_list={group_list}``
        self.group_list: Union[str, None] = None

        #: Requested memory size on the select line. Need to include units in the str.
        #: The associated PBS header line is ``#PBS -l select=#:mem={mem}``
        self.mem: Union[str, None] = mem

        #: Index range for PBS array of jobs
        #: The associated PBS header line is ``#PBS -J {array_range}``
        self.array_range: Union[str, None] = None

        #: ``pbs -m`` mail options. 'e' at exit, 'b' at beginning, 'a' at abort
        self.mail_options: str = None

        #: ``pbs -M`` mail list. Who to email when mail_options are triggered
        self.mail_list: Union[str, None] = None

        #: Type of dependency if dependency active.
        #: Default is 'afterok' which only launches the new job if the previous one was successful.
        self.dependency_type: str = "afterok"

        self.mpiexec: str = "mpiexec"
        self.ranks_per_node_flag = None

        self.workdir_env_variable = "$PBS_O_WORKDIR"
        self.batch_file_extension = "pbs"
        self.requested_number_of_nodes = requested_number_of_nodes

    def _create_list_of_standard_header_options(self, job_name: str) -> List[str]:
        header_lines = [
            self._create_hashbang(),
            self._create_job_line_of_header(job_name),
            self._create_queue_line_of_header(),
            self._create_select_line_of_header(),
            self._create_walltime_line_of_header(),
            self._create_log_name_line_of_header(job_name),
            self._create_header_line_to_join_standard_and_error_output(),
            self._create_header_line_to_set_that_job_is_not_rerunnable(),
        ]
        return header_lines

    def _create_job_line_of_header(self, job_name: str) -> str:
        return f"#PBS -N {job_name}"

    def _create_queue_line_of_header(self) -> str:
        return f"#PBS -q {self.queue_name}"

    def _create_select_line_of_header(self) -> str:
        select = f"select={self.requested_number_of_nodes}"
        ncpus = f"ncpus={self.ncpus_per_node}"
        mpiprocs = f"mpiprocs={self.mpiprocs_per_node}"

        select_line = f"#PBS -l {select}:{ncpus}"
        if self.ngpus_per_node > 0:
            select_line += f":ngpus={self.ngpus_per_node}"
        select_line += f":{mpiprocs}"
        if self.mem is not None:
            select_line += f":mem={self.mem}"
        if self.model is not None:
            select_line += f":model={self.model}"
        return select_line

    def _create_walltime_line_of_header(self) -> str:
        return f"#PBS -l walltime={self.time}:00:00"

    def _create_log_name_line_of_header(self, job_name: str) -> str:
        return f"#PBS -o {job_name}_pbs.log"

    def _create_header_line_to_join_standard_and_error_output(self):
        return "#PBS -j oe"

    def _create_header_line_to_set_that_job_is_not_rerunnable(self) -> str:
        return "#PBS -r n"

    def _create_list_of_optional_header_lines(self, dependency):
        header_lines = []
        header_lines.extend(self._create_group_list_header_line())
        header_lines.extend(self._create_array_range_header_line())
        header_lines.extend(self._create_mail_options_header_lines())
        header_lines.extend(self._create_job_dependencies_header_line(dependency))
        return header_lines

    def _create_group_list_header_line(self) -> List[str]:
        if self.group_list is not None:
            return [f"#PBS -W group_list={self.group_list}"]
        else:
            return []

    def _create_array_range_header_line(self) -> List[str]:
        if self.array_range is not None:
            return [f"#PBS -J {self.array_range}"]
        else:
            return []

    def _create_mail_options_header_lines(self) -> List[str]:
        header_lines = []
        if self.mail_options is not None:
            header_lines.append(f"#PBS -m {self.mail_options}")
        if self.mail_list is not None:
            header_lines.append(f"#PBS -M {self.mail_list}")
        return header_lines

    def _create_job_dependencies_header_line(self, dependency) -> List[str]:
        if dependency is not None:
            return [f"#PBS -W depend={self.dependency_type}:{dependency}"]
        else:
            return []

    def _run_job(self, job_filename: str, blocking: bool, print_command_output=True) -> str:
        options = ""
        if blocking:
            options += "-W block=true"
        command_output = os.popen(f"qsub {options} {job_filename}").read().strip()
        if print_command_output:
            print(command_output)
        return command_output

    # Alternate constructors for NASA HPC queues
    @classmethod
    def k4(cls, time: int = 72, profile_filename: str = "~/.bashrc", requested_number_of_nodes: int = 1):
        """
        Constructor for the K4 queues on LaRC's K cluster including K4-standard-512.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        requested_number_of_nodes:
            The number of compute nodes to request
        """
        return cls(
            queue_name="K4-route",
            ncpus_per_node=40,
            queue_node_limit=16,
            time=time,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def k3c(cls, time: int = 72, profile_filename: str = "~/.bashrc", requested_number_of_nodes: int = 1):
        """
        Constructor for the K3b queues on LaRC's K cluster.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        requested_number_of_nodes:
            The number of compute nodes to request
        """
        return cls(
            queue_name="K3c-route",
            ncpus_per_node=28,
            queue_node_limit=74,
            time=time,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def k3b(cls, time: int = 72, profile_filename: str = "~/.bashrc", requested_number_of_nodes: int = 1):
        """
        Constructor for the K3b queues on LaRC's K cluster.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        requested_number_of_nodes:
            The number of compute nodes to request
        """
        return cls(
            queue_name="K3b-route",
            ncpus_per_node=28,
            queue_node_limit=74,
            time=time,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def k3a(cls, time: int = 72, profile_filename: str = "~/.bashrc", requested_number_of_nodes: int = 1):
        """
        Constructor for the K3a queue on LaRC's K cluster.

        Parameters
        ----------
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        requested_number_of_nodes:
            The number of compute nodes to request
        """
        return cls(
            queue_name="K3a-route",
            ncpus_per_node=16,
            queue_node_limit=25,
            time=time,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def k4_v100(
        cls,
        time: int = 72,
        ncpus_per_node=0,
        ngpus_per_node=4,
        mem="200G",
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
    ):
        if ncpus_per_node == 0:
            ncpus_per_node = ngpus_per_node
        return cls(
            queue_name="K4-V100",
            ncpus_per_node=ncpus_per_node,
            ngpus_per_node=ngpus_per_node,
            queue_node_limit=4,
            time=time,
            mem=mem,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def k5_a100_80(
        cls,
        time: int = 72,
        ncpus_per_node=0,
        ngpus_per_node=8,
        mem="700G",
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
    ):
        if ncpus_per_node == 0:
            ncpus_per_node = ngpus_per_node
        return cls(
            queue_name="K5-A100-80",
            ncpus_per_node=ncpus_per_node,
            ngpus_per_node=ngpus_per_node,
            queue_node_limit=2,
            time=time,
            mem=mem,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def k5_a100_40(
        cls,
        time: int = 72,
        ncpus_per_node=0,
        ngpus_per_node=8,
        mem="700G",
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
    ):
        if ncpus_per_node == 0:
            ncpus_per_node = ngpus_per_node
        return cls(
            queue_name="K5-A100-40",
            ncpus_per_node=ncpus_per_node,
            ngpus_per_node=ngpus_per_node,
            queue_node_limit=2,
            time=time,
            mem=mem,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

    @classmethod
    def nas(
        cls,
        group_list: str,
        proc_type: str = "broadwell",
        queue_name: str = "long",
        time: int = 72,
        mem: str = None,
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
    ):
        """
        Constructor for the queues at NAS. Must specify the group_list

        Parameters
        ----------
        group_list:
            The charge number or group for the group_list entry of the pbs header.
            The associated PBS header line is "#PBS -W group_list={group_list}".
        proc_type:
            The type of processor to submit to. Can write out or just the first 3 letters:
            'cas', 'sky', 'bro', 'has', 'ivy', 'san'.
        queue_name:
            Which queue to submit to: devel, debug, normal, long, etc.
        time:
            The requested job walltime in hours
        profile_file:
            The file setting the environment to source inside the PBS job
        """
        if "sky_gpu" in proc_type.lower():
            ncpus_per_node = 36
            ngpus_per_node = 4
            model = "sky_gpu"
            mem = "200G"
        elif "cas_gpu" in proc_type.lower():
            ncpus_per_node = 48
            ngpus_per_node = 4
            model = "cas_gpu"
            mem = "200G"
        elif "rom_gpu" in proc_type.lower():
            ncpus_per_node = 128
            ngpus_per_node = 8
            model = "rom_gpu"
            mem = "700G"
        elif "mil_a100" in proc_type.lower():
            ncpus_per_node = 64
            ngpus_per_node = 4
            model = "mil_a100"
            mem = "500G"
        elif "cas" in proc_type.lower():
            ncpus_per_node = 40
            ngpus_per_node = 0
            model = "cas_ait"
        elif "sky" in proc_type.lower():
            ncpus_per_node = 40
            ngpus_per_node = 0
            model = "sky_ele"
        elif "bro" in proc_type.lower():
            ncpus_per_node = 28
            ngpus_per_node = 0
            model = "bro"
        elif "has" in proc_type.lower():
            ncpus_per_node = 24
            ngpus_per_node = 0
            model = "has"
        elif "ivy" in proc_type.lower():
            ncpus_per_node = 20
            ngpus_per_node = 0
            model = "ivy"
        elif "san" in proc_type.lower():
            ncpus_per_node = 16
            ngpus_per_node = 0
            model = "san"
        elif "rom" in proc_type.lower():
            ncpus_per_node = 128
            ngpus_per_node = 0
            model = "rom_ait"
        elif "mil" in proc_type.lower():
            ncpus_per_node = 128
            ngpus_per_node = 0
            model = "mil_ait"
        else:
            raise ValueError("Unknown NAS processor selection")

        pbs = cls(
            queue_name=queue_name,
            ncpus_per_node=ncpus_per_node,
            ngpus_per_node=ngpus_per_node,
            queue_node_limit=int(1e6),
            time=time,
            mem=mem,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )

        pbs.group_list = group_list
        pbs.model = model
        return pbs

    @classmethod
    def cf1(
        cls,
        account: str,
        queue_name: str = "normal",
        queue_node_limit: int = 30,
        time: int = 24,
        ncpus_per_node=64,
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 2,
    ):
        pbs = cls(
            queue_name=queue_name,
            queue_node_limit=queue_node_limit,
            ncpus_per_node=ncpus_per_node,
            time=time,
            profile_filename=profile_filename,
            requested_number_of_nodes=requested_number_of_nodes,
        )
        pbs.group_list = account
        pbs.workdir_env_variable = "$SLURM_SUBMIT_DIR"
        return pbs

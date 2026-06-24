#!/usr/bin/env python
import os
from typing import List, Union

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
        model: str = None,
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
        profile_filename:
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
        self.model = model

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

        #: scatter placement, for example excl would be "#place=scatter:excl"
        self.place = None

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
        header_lines.extend(self._create_place_line())
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

    def _create_place_line(self):
        if self.place is not None:
            return [f"#PBS -l place=scatter:{self.place}"]
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
    def k4(cls, **kwargs):
        """
        Constructor for the K4 queues on LaRC's K cluster including K4-standard-512.
        """
        defaults = {
            'queue_name': "K4-route",
            'ncpus_per_node': 40,
            'queue_node_limit': 16,
        }
        return cls(**{**defaults, **kwargs})

    @classmethod
    def k3b(cls, **kwargs):
        """
        Constructor for the K3b queues on LaRC's K cluster.
        """
        defaults = {
            'queue_name': "K3b-route",
            'ncpus_per_node': 28,
            'queue_node_limit': 74,
        }
        return cls(**{**defaults, **kwargs})

    @classmethod
    def k4_v100(cls, **kwargs):
        """
        Constructor for the K4-V100 GPU queue on LaRC's K cluster.
        """
        # set ncpus_per_node as ngpus if not directly set
        if kwargs.get('ncpus_per_node', 0) == 0:
            kwargs['ncpus_per_node'] = kwargs.get('ngpus_per_node', 4)

        defaults = {
            'queue_name': "K4-V100",
            'ngpus_per_node': 4,
            'queue_node_limit': 4,
            'mem': "200G",
        }
        return cls(**{**defaults, **kwargs})

    @classmethod
    def k5_a100_80(cls, **kwargs):
        """
        Constructor for the K5-A100-80 GPU queue on LaRC's K cluster.
        """
        # set ncpus_per_node as ngpus if not directly set
        if kwargs.get('ncpus_per_node', 0) == 0:
            kwargs['ncpus_per_node'] = kwargs.get('ngpus_per_node', 8)

        defaults = {
            'queue_name': "K5-A100-80",
            'ngpus_per_node': 8,
            'queue_node_limit': 2,
            'mem': "700G",
        }
        return cls(**{**defaults, **kwargs})

    @classmethod
    def k5_a100_40(cls, **kwargs):
        """
        Constructor for the K5-A100-40 GPU queue on LaRC's K cluster.
        """
        # set ncpus_per_node as ngpus if not directly set
        if kwargs.get('ncpus_per_node', 0) == 0:
            kwargs['ncpus_per_node'] = kwargs.get('ngpus_per_node', 8)

        defaults = {
            'queue_name': "K5-A100-40",
            'ngpus_per_node': 8,
            'queue_node_limit': 2,
            'mem': "700G",
        }
        return cls(**{**defaults, **kwargs})

    @classmethod
    def nas(cls, group_list: str, proc_type: str = "rome", **kwargs):
        """
        Constructor for the queues at NAS. Must specify the group_list

        Parameters
        ----------
        group_list:
            The charge number or group for the group_list entry of the pbs header.
            The associated PBS header line is "#PBS -W group_list={group_list}".
        proc_type:
            The type of processor to submit to. Can write out or just the first 3 letters:
            'cas', 'sky', 'bro', 'has', 'ivy', 'san', 'rom', 'mil', 'tur'.
        """

        proc_configs = {
            'sky_gpu': {'ncpus_per_node': 36, 'ngpus_per_node': 4, 'model': 'sky_gpu', 'mem': '200G'},
            'cas_gpu': {'ncpus_per_node': 48, 'ngpus_per_node': 4, 'model': 'cas_gpu', 'mem': '200G'},
            'rom_gpu': {'ncpus_per_node': 128, 'ngpus_per_node': 8, 'model': 'rom_gpu', 'mem': '700G'},
            'mil_a100': {'ncpus_per_node': 64, 'ngpus_per_node': 4, 'model': 'mil_a100', 'mem': '500G'},
            'cas': {'ncpus_per_node': 40,  'model': 'cas_ait'},
            'sky': {'ncpus_per_node': 40,  'model': 'sky_ele'},
            'bro': {'ncpus_per_node': 28,  'model': 'bro'},
            'rom': {'ncpus_per_node': 128,  'model': 'rom_ait'},
            'mil': {'ncpus_per_node': 128,  'model': 'mil_ait'},
            'tur': {'ncpus_per_node': 256,  'model': 'tur_ath'},
        }

        # map proc_type to configuration
        proc_lower = proc_type.lower()
        config = None
        for key in ['sky_gpu', 'cas_gpu', 'rom_gpu', 'mil_a100']:
            if key in proc_lower:
                config = proc_configs[key]
                break

        if config is None:
            for key in ['cas', 'sky', 'bro', 'rom', 'mil', 'tur']:
                if key in proc_lower:
                    config = proc_configs[key]
                    break

        if config is None:
            raise ValueError(f"Unknown NAS processor selection: {proc_type}")

        defaults = {
            'queue_name': 'long',
            'queue_node_limit': int(1e6),
            **config
        }

        pbs = cls(**{**defaults, **kwargs})
        pbs.group_list = group_list
        pbs.model = config['model']
        return pbs

    @classmethod
    def cf1(cls, account: str, **kwargs):
        """
        Constructor for the CF1 cluster.

        Parameters
        ----------
        account:
            The account/group for the group_list entry
        """
        defaults = {
            'queue_name': 'normal',
            'queue_node_limit': 30,
            'time': 24,
            'ncpus_per_node': 64,
            'requested_number_of_nodes': 2,
        }

        pbs = cls(**{**defaults, **kwargs})
        pbs.group_list = account
        pbs.workdir_env_variable = "$SLURM_SUBMIT_DIR"
        return pbs

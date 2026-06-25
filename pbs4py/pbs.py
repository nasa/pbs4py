import subprocess

from pbs4py.launcher_base import Launcher

_NO_NODE_LIMIT = 1_000_000


class PBS(Launcher):
    def __init__(
        self,
        queue_name: str = "K4-route",
        ncpus_per_node: int = 40,
        ngpus_per_node: int = 0,
        queue_node_limit: int = 10,
        time: int = 72,
        mem: str | None = None,
        profile_filename: str = "~/.bashrc",
        requested_number_of_nodes: int = 1,
        model: str | None = None,
    ):
        """
        | A class for creating and running pbs jobs. Default queue properties are for K4.
        | Defaults not set during instantiation can be adjusted by directly modifying attributes.

        Parameters
        ----------
        queue_name:
            Queue name which goes on the ``#PBS -q {queue_name}`` line of the pbs header
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
        model:
            The processor model. If left as ``None``, ``:model={model}`` will not be
            added to the select line.
        """
        super().__init__(
            ncpus_per_node,
            ngpus_per_node,
            queue_node_limit,
            time,
            profile_filename,
            requested_number_of_nodes,
        )

        #: The name of the queue, used on the ``#PBS -q {queue_name}`` header line.
        self.queue_name: str = queue_name

        #: The processor model if it needs to be specified.
        #: Appears on the select line as ``:model={model}`` when not ``None``.
        self.model: str | None = model

        #: The group for the ``#PBS -W group_list={group_list}`` header line.
        self.group_list: str | None = None

        #: Requested memory size on the select line. Include units in the string.
        self.mem: str | None = mem

        #: Index range for a PBS job array. Header line: ``#PBS -J {array_range}``
        self.array_range: str | None = None

        #: ``pbs -m`` mail options. 'e' at exit, 'b' at beginning, 'a' at abort.
        self.mail_options: str | None = None

        #: ``pbs -M`` mail list. Who to email when mail_options are triggered.
        self.mail_list: str | None = None

        #: Type of dependency if dependency active.
        #: Default 'afterok' only launches the new job if the previous one succeeded.
        self.dependency_type: str = "afterok"

        self.mpiexec: str = "mpiexec"
        self.ranks_per_node_flag: str | None = None

        self.workdir_env_variable: str = "$PBS_O_WORKDIR"
        self.batch_file_extension: str = "pbs"

        #: Scatter placement, e.g. ``"excl"`` produces ``#PBS -l place=scatter:excl``.
        self.place: str | None = None

    # ------------------------------------------------------------------
    # Header construction
    # ------------------------------------------------------------------
    def _create_list_of_standard_header_options(self, job_name: str) -> list[str]:
        return [
            self._create_hashbang(),
            self._create_job_line_of_header(job_name),
            self._create_queue_line_of_header(),
            self._create_select_line_of_header(),
            self._create_walltime_line_of_header(),
            self._create_log_name_line_of_header(job_name),
            self._create_header_line_to_join_standard_and_error_output(),
            self._create_header_line_to_set_that_job_is_not_rerunnable(),
        ]

    def _create_job_line_of_header(self, job_name: str) -> str:
        return f"#PBS -N {job_name}"

    def _create_queue_line_of_header(self) -> str:
        return f"#PBS -q {self.queue_name}"

    def _create_select_line_of_header(self) -> str:
        parts = [
            f"select={self.requested_number_of_nodes}",
            f"ncpus={self.ncpus_per_node}",
        ]
        if self.ngpus_per_node > 0:
            parts.append(f"ngpus={self.ngpus_per_node}")
        parts.append(f"mpiprocs={self.mpiprocs_per_node}")
        if self.mem is not None:
            parts.append(f"mem={self.mem}")
        if self.model is not None:
            parts.append(f"model={self.model}")
        return "#PBS -l " + ":".join(parts)

    def _create_walltime_line_of_header(self) -> str:
        return f"#PBS -l walltime={self.time}:00:00"

    def _create_log_name_line_of_header(self, job_name: str) -> str:
        return f"#PBS -o {job_name}_pbs.log"

    def _create_header_line_to_join_standard_and_error_output(self) -> str:
        return "#PBS -j oe"

    def _create_header_line_to_set_that_job_is_not_rerunnable(self) -> str:
        return "#PBS -r n"

    def _create_list_of_optional_header_lines(self, dependency: str | None) -> list[str]:
        return [
            *self._create_group_list_header_line(),
            *self._create_array_range_header_line(),
            *self._create_place_line(),
            *self._create_mail_options_header_lines(),
            *self._create_job_dependencies_header_line(dependency),
        ]

    def _create_group_list_header_line(self) -> list[str]:
        return [f"#PBS -W group_list={self.group_list}"] if self.group_list else []

    def _create_array_range_header_line(self) -> list[str]:
        return [f"#PBS -J {self.array_range}"] if self.array_range else []

    def _create_place_line(self) -> list[str]:
        return [f"#PBS -l place=scatter:{self.place}"] if self.place else []

    def _create_mail_options_header_lines(self) -> list[str]:
        header_lines = []
        if self.mail_options is not None:
            header_lines.append(f"#PBS -m {self.mail_options}")
        if self.mail_list is not None:
            header_lines.append(f"#PBS -M {self.mail_list}")
        return header_lines

    def _create_job_dependencies_header_line(self, dependency: str | None) -> list[str]:
        if dependency is None:
            return []
        return [f"#PBS -W depend={self.dependency_type}:{dependency}"]

    # ------------------------------------------------------------------
    # Job submission
    # ------------------------------------------------------------------
    def _run_job(self, job_filename: str, blocking: bool, print_command_output: bool = True) -> str:
        cmd = ["qsub"]
        if blocking:
            cmd += ["-W", "block=true"]
        cmd.append(job_filename)

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        output = result.stdout.strip()
        if print_command_output:
            if output:
                print(output)
            if result.stderr:
                print(result.stderr.strip())
        return output

    # ------------------------------------------------------------------
    # Alternate constructors for NASA HPC queues
    # ------------------------------------------------------------------
    @classmethod
    def k4(cls, **kwargs) -> "PBS":
        """Constructor for the K4 queues on LaRC's K cluster, including K4-standard-512."""
        defaults = {
            "queue_name": "K4-route",
            "ncpus_per_node": 40,
            "queue_node_limit": 16,
        }
        return cls(**(defaults | kwargs))

    @classmethod
    def k3b(cls, **kwargs) -> "PBS":
        """Constructor for the K3b queues on LaRC's K cluster."""
        defaults = {
            "queue_name": "K3b-route",
            "ncpus_per_node": 28,
            "queue_node_limit": 74,
        }
        return cls(**(defaults | kwargs))

    @classmethod
    def _gpu_queue(
        cls,
        queue_name: str,
        ngpus_per_node: int,
        queue_node_limit: int,
        mem: str,
        **kwargs,
    ) -> "PBS":
        """Shared helper for K-cluster GPU-queue constructors."""
        # If the user did not specify ncpus, match it to ngpus.
        kwargs.setdefault("ncpus_per_node", kwargs.get("ngpus_per_node", ngpus_per_node))
        defaults = {
            "queue_name": queue_name,
            "ngpus_per_node": ngpus_per_node,
            "queue_node_limit": queue_node_limit,
            "mem": mem,
        }
        return cls(**(defaults | kwargs))

    @classmethod
    def k4_v100(cls, **kwargs) -> "PBS":
        """Constructor for the K4-V100 GPU queue on LaRC's K cluster."""
        return cls._gpu_queue("K4-V100", ngpus_per_node=4, queue_node_limit=4, mem="200G", **kwargs)

    @classmethod
    def k5_a100_80(cls, **kwargs) -> "PBS":
        """Constructor for the K5-A100-80 GPU queue on LaRC's K cluster."""
        return cls._gpu_queue(
            "K5-A100-80", ngpus_per_node=8, queue_node_limit=2, mem="700G", **kwargs
        )

    @classmethod
    def k5_a100_40(cls, **kwargs) -> "PBS":
        """Constructor for the K5-A100-40 GPU queue on LaRC's K cluster."""
        return cls._gpu_queue(
            "K5-A100-40", ngpus_per_node=8, queue_node_limit=2, mem="700G", **kwargs
        )

    @classmethod
    def nas(cls, group_list: str, proc_type: str = "rome", **kwargs) -> "PBS":
        """
        Constructor for the queues at NAS. Must specify the group_list.

        Parameters
        ----------
        group_list:
            The charge number or group for the group_list entry of the pbs header.
            The associated PBS header line is ``#PBS -W group_list={group_list}``.
        proc_type:
            The type of processor to submit to. Can write out or just the first 3 letters:
            'cas', 'sky', 'bro', 'rom', 'mil', 'tur'. GPU nodes can be selected with
            'sky_gpu', 'cas_gpu', 'rom_gpu', or 'mil_a100'.
        """
        proc_configs = {
            "sky_gpu": {
                "ncpus_per_node": 36,
                "ngpus_per_node": 4,
                "model": "sky_gpu",
                "mem": "200G",
            },
            "cas_gpu": {
                "ncpus_per_node": 48,
                "ngpus_per_node": 4,
                "model": "cas_gpu",
                "mem": "200G",
            },
            "rom_gpu": {
                "ncpus_per_node": 128,
                "ngpus_per_node": 8,
                "model": "rom_gpu",
                "mem": "700G",
            },
            "mil_a100": {
                "ncpus_per_node": 64,
                "ngpus_per_node": 4,
                "model": "mil_a100",
                "mem": "500G",
            },
            "cas": {"ncpus_per_node": 40, "model": "cas_ait"},
            "sky": {"ncpus_per_node": 40, "model": "sky_ele"},
            "bro": {"ncpus_per_node": 28, "model": "bro"},
            "rom": {"ncpus_per_node": 128, "model": "rom_ait"},
            "mil": {"ncpus_per_node": 128, "model": "mil_ait"},
            "tur": {"ncpus_per_node": 256, "model": "tur_ath"},
        }

        # Map proc_type to a configuration. Check GPU/accelerator keys first so that,
        # e.g., 'rom_gpu' isn't matched as 'rom'.
        proc_lower = proc_type.lower()
        config = None
        for key in ("sky_gpu", "cas_gpu", "rom_gpu", "mil_a100"):
            if key in proc_lower:
                config = proc_configs[key]
                break

        if config is None:
            for key in ("cas", "sky", "bro", "rom", "mil", "tur"):
                if key in proc_lower:
                    config = proc_configs[key]
                    break

        if config is None:
            raise ValueError(f"Unknown NAS processor selection: {proc_type!r}")

        defaults = {
            "queue_name": "long",
            "queue_node_limit": _NO_NODE_LIMIT,
            **config,
        }

        pbs = cls(**(defaults | kwargs))
        pbs.group_list = group_list
        return pbs

    @classmethod
    def cf1(cls, account: str, **kwargs) -> "PBS":
        """
        Constructor for the CF1 cluster.

        Parameters
        ----------
        account:
            The account/group for the group_list entry.
        """
        defaults = {
            "queue_name": "normal",
            "queue_node_limit": 30,
            "time": 24,
            "ncpus_per_node": 64,
            "requested_number_of_nodes": 2,
        }

        pbs = cls(**(defaults | kwargs))
        pbs.group_list = account
        pbs.workdir_env_variable = "$SLURM_SUBMIT_DIR"
        return pbs

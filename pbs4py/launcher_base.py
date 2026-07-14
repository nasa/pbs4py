import subprocess
from pathlib import Path


class Launcher:
    def __init__(
        self,
        ncpus_per_node: int,
        ngpus_per_node: int,
        queue_node_limit: int,
        time: int,
        profile_filename: str,
        requested_number_of_nodes: int,
    ):
        #: The hashbang line which sets the shell for the PBS script.
        #: If unset, the default is ``#!/usr/bin/env {self.shell}``.
        self.hashbang: str | None = None

        #: The shell flavor to use in the PBS job
        self.shell: str = "bash"

        #: The maximum number of nodes allowed by the queue
        self.queue_node_limit: int = queue_node_limit

        #: The number of compute nodes requested
        self.requested_number_of_nodes: int = requested_number_of_nodes

        #: The requested wall time for the pbs job(s) in hours
        self.time: int = time

        #: The number of CPU cores per node.
        self.ncpus_per_node: int = ncpus_per_node

        #: The number of GPUs per node.
        self.ngpus_per_node: int = ngpus_per_node

        #: The mpi execution command name: mpiexec, mpirun, mpiexec_mpt, etc.
        self.mpiexec: str = "mpiexec"
        self._mpiprocs_per_node: int | None = None

        #: Command line option for mpiexec to specify the number of MPI ranks per host/node.
        #: Default is to set it based on the mpiexec version.
        self.ranks_per_node_flag: str | None = None

        # These are properties that users typically don't need to set, but are
        # specific to each queueing software.
        self.workdir_env_variable: str = ""
        self.batch_file_extension: str = ""

        #: If true, redirection of the output of mpi commands is changed to tee.
        self.tee_output: bool = False

        self.profile_filename = profile_filename

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    @property
    def requested_number_of_nodes(self) -> int:
        """
        The number of nodes to request. That is, the 'select' number in the
        ``#PBS -l select={requested_number_of_nodes}:ncpus=40:mpiprocs=40``.

        Note: values larger than ``queue_node_limit`` are clamped.

        :type: int
        """
        return self._requested_number_of_nodes

    @requested_number_of_nodes.setter
    def requested_number_of_nodes(self, number_of_nodes: int) -> None:
        self._requested_number_of_nodes = min(number_of_nodes, self.queue_node_limit)

    @property
    def mpiprocs_per_node(self) -> int:
        """
        The number of requested mpiprocs per node. If not set, the launcher will default
        to the number of cpus per node.
        ``#PBS -l select=1:ncpus=40:mpiprocs={mpiprocs_per_node}``.

        :type: int
        """
        if self._mpiprocs_per_node is None:
            return self.ncpus_per_node
        return self._mpiprocs_per_node

    @mpiprocs_per_node.setter
    def mpiprocs_per_node(self, mpiprocs: int | None) -> None:
        self._mpiprocs_per_node = mpiprocs

    @property
    def profile_filename(self) -> str:
        """
        The file to source at the start of the pbs script to set the environment.
        Typical names include '~/.profile', '~/.bashrc', and '~/.cshrc'.
        If you do not wish to source a file, set to ''.

        :type: str
        """
        return self._profile_filename

    @profile_filename.setter
    def profile_filename(self, profile_filename: str) -> None:
        if profile_filename == "" or Path(profile_filename).expanduser().is_file():
            self._profile_filename = profile_filename
        else:
            raise FileNotFoundError(f"Unable to set profile file: {profile_filename!r}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def create_mpi_command(
        self,
        command: str,
        output_root_name: str | None = None,
        openmp_threads: int | None = None,
        ranks_per_node: int | None = None,
    ) -> str:
        """
        Wrap a command with mpiexec and route its standard and error output to a file.

        Parameters
        ----------
        command:
            The command that needs to run in parallel.
        output_root_name:
            The root name of the output file, ``{output_root_name}.out``.
        openmp_threads:
            The number of openmp threads per mpi process.
        ranks_per_node:
            The number of MPI ranks per compute node.

        Returns
        -------
        full_command: str
            The full command string.
        """
        omp_env_vars = self._determine_omp_settings(openmp_threads)
        ranks_per_node_info = self._set_ranks_per_node_info(openmp_threads, ranks_per_node)
        openmp_info = self._set_openmp_info(openmp_threads)

        full_command = [omp_env_vars, self.mpiexec, ranks_per_node_info, openmp_info, command]
        if output_root_name is not None:
            full_command.append(self._redirect_shell_output(f"{output_root_name}.out"))
        return self._filter_empty_strings_from_list_and_combine(full_command)

    def launch(
        self,
        job_name: str,
        job_body: list[str],
        blocking: bool = True,
        dependency: str | None = None,
    ) -> str:
        """
        Create a job script and launch the job.

        Parameters
        ----------
        job_name:
            The name of the job.
        job_body:
            List of commands to run in the body of the job.
        blocking:
            If true, this function will wait for the job to complete before returning.
            If false, this function will launch the job but not wait for it to finish.
        dependency:
            Jobs that this one depends on. For PBS, these are colon separated in the string.

        Returns
        -------
        command_output: str
            The stdout of the launch command. If the job is successfully launched,
            this will be the job id.
        """
        filename = f"{job_name}.{self.batch_file_extension}"
        self.write_job_file(filename, job_name, job_body, dependency)
        return self._run_job(filename, blocking)

    def write_job_file(
        self,
        job_filename: str,
        job_name: str,
        job_body: list[str],
        dependency: str | None = None,
    ) -> None:
        """
        Create a launch script file in the current directory for the commands defined in ``job_body``.

        Parameters
        ----------
        job_filename:
            Name of file to write to.
        job_name:
            The name of the job.
        job_body:
            List of commands to run in the body of the job.
        dependency:
            Jobs that this one depends on. For PBS, these are colon separated in the string.
        """
        header = self._create_header(job_name, dependency)

        lines = [
            *header,
            "",
            "",
            f"cd {self.workdir_env_variable}",
        ]
        if self.profile_filename:
            lines.append(f"source {self.profile_filename}")
        lines.append("")
        lines.extend(job_body)

        with open(job_filename, mode="w") as fh:
            fh.write("\n".join(lines) + "\n")

    # ------------------------------------------------------------------
    # Header construction (intended to be overridden by subclasses)
    # ------------------------------------------------------------------
    def _create_header(self, job_name: str, dependency: str | None = None) -> list[str]:
        header = self._create_list_of_standard_header_options(job_name)
        header.extend(self._create_list_of_optional_header_lines(dependency))
        return header

    def _create_hashbang(self) -> str:
        if self.hashbang is not None:
            return self.hashbang
        return f"#!/usr/bin/env {self.shell}"

    def _create_list_of_standard_header_options(self, job_name: str) -> list[str]:
        return []

    def _create_list_of_optional_header_lines(self, dependency: str | None) -> list[str]:
        return []

    def _run_job(self, job_filename: str, blocking: bool, print_command_output: bool = True) -> str:
        raise NotImplementedError("Launcher must implement a _run_job method")

    # ------------------------------------------------------------------
    # MPI / OpenMP helpers
    # ------------------------------------------------------------------
    def _redirect_shell_output(self, output_filename: str) -> str:
        if self.tee_output:
            return f"2>&1 | tee {output_filename}"
        if self.shell == "tcsh":
            return f">& {output_filename}"
        return f"&> {output_filename}"

    def _use_omplace_command(self) -> bool:
        return self._using_mpt()

    def _use_openmp(self, openmp_threads: int | None = None) -> bool:
        return openmp_threads is not None and openmp_threads > 1

    def _using_mpt(self) -> bool:
        if self.mpiexec == "mpiexec_mpt":
            return True

        try:
            output = subprocess.run(
                [self.mpiexec, "--version"],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return False
        return "MPT" in output.stderr or "MPT" in output.stdout

    def _get_ranks_per_node_flag(self) -> str:
        if self.ranks_per_node_flag is not None:
            return self.ranks_per_node_flag
        return "-perhost" if self._using_mpt() else "--npernode"

    def _determine_omp_settings(self, openmp_threads: int | None) -> str:
        if openmp_threads is None:
            return ""

        omp_env_vars = [f"OMP_NUM_THREADS={openmp_threads}"]
        if not self._use_omplace_command():
            omp_env_vars.extend(["OMP_PLACES=cores", "OMP_PROC_BIND=close"])
        return self._filter_empty_strings_from_list_and_combine(omp_env_vars)

    def _filter_empty_strings_from_list_and_combine(self, lis: list[str]) -> str:
        return " ".join(filter(None, lis))

    def _set_ranks_per_node_info(
        self, openmp_threads: int | None, ranks_per_node: int | None
    ) -> str:
        if ranks_per_node is None and openmp_threads is None:
            return ""
        if ranks_per_node is not None:
            mpi_procs_per_node = ranks_per_node
        else:
            mpi_procs_per_node = self.ncpus_per_node // openmp_threads
        return f"{self._get_ranks_per_node_flag()} {mpi_procs_per_node}"

    def _set_openmp_info(self, openmp_threads: int | None) -> str:
        if not self._use_openmp(openmp_threads):
            return ""
        if not self._use_omplace_command():
            return ""
        proc_num_list = ",".join(str(i) for i in range(self.ncpus_per_node))
        return f'omplace -c "{proc_num_list}" -nt {openmp_threads} -vv'

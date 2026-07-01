import os
import subprocess
import time
from datetime import datetime


class PBSJob:
    def __init__(self, id: str):
        """
        A class for querying information and managing a particular submitted
        PBS job. For the id number in the constructor, the ``qstat`` command
        will be used to populate the attributes of the job.

        Parameters
        ----------
        id:
            The id of the PBS job.
        """
        #: The ID of the PBS job.
        self.id: str = id

        #: The name of the job.
        self.name: str = ""

        #: The model attribute on the select line from the job submission.
        self.model: str = ""

        #: The number of resources on the select line.
        self.requested_number_of_nodes: int = 0

        #: The number of CPUs per node.
        self.ncpus_per_node: int = 0

        #: The queue which this job was submitted to.
        self.queue: str = ""

        #: Whether the job is queued, running, or finished.
        self.state: str = ""

        #: The value of $PBS_O_WORKDIR.
        self.workdir: str = ""

        #: The exit status of the PBS job.
        self.exit_status: int | None = None

        #: Requested walltime in seconds.
        self.walltime_requested: int | None = None

        #: Walltime used so far in seconds (None if job hasn't started).
        self.walltime_used: int | None = None

        #: Walltime remaining in seconds (None if job hasn't started).
        self.walltime_remaining: int | None = None

        #: Hostname of the executing node (empty if job hasn't started).
        self.hostname: str = ""

        #: Raw mtime string from qstat (e.g., "Thu Oct 24 12:26:32 2024" or
        #: "1649348653 (Thu Apr 07 12:24:13 EDT 2022)").
        self.mtime_raw: str = ""

        #: Parsed mtime as a datetime (None if not available or unparseable).
        self.mtime: datetime | None = None

        self.read_properties_from_qstat()

    def read_properties_from_qstat(self) -> None:
        """Use ``qstat`` to get the current attributes of this job."""
        if self._this_job_was_launched_from_fake_pbs():
            self._read_properties_from_fake_pbs_launcher_job()
        else:
            self._read_properties_of_real_pbs_job()

    def _read_properties_of_real_pbs_job(self) -> None:
        qstat_output = self._run_qstat_to_get_full_job_attributes()
        if self._is_a_known_job(qstat_output):
            self._parse_attributes_from_qstat_output(qstat_output)
        else:
            self._set_empty_attributes()

    def _read_properties_from_fake_pbs_launcher_job(self) -> None:
        self.exit_status = int(self.id.split(".")[-1])

    def qdel(self, echo_command: bool = True) -> str:
        """
        Call ``qdel`` to delete this job.

        Parameters
        ----------
        echo_command:
            Whether to print the command before running it.

        Returns
        -------
        The output of the shell command.
        """
        command = f"qdel {self.id}"
        if echo_command:
            print(command)
        return os.popen(command).read()

    def tail_file_until_job_is_finished(self, file_to_tail: str) -> None:
        """
        Print the contents of a file as it grows, until this job finishes.

        Parameters
        ----------
        file_to_tail:
            Path to the file to tail.
        """
        if self._this_job_was_launched_from_fake_pbs():
            self._print_entire_file(file_to_tail)
            return

        # Touch the file so we can open it even if the job hasn't created it yet.
        if not os.path.exists(file_to_tail):
            open(file_to_tail, "w").close()

        with open(file_to_tail, "r") as file:
            for line in file:
                print(line, end="")

            # Then follow it.
            while True:
                line = file.readline()
                if line:
                    print(line, end="")
                    continue

                time.sleep(0.1)
                if not self._job_is_still_running_or_queued():
                    # Drain anything written between our last read and now.
                    for line in file:
                        print(line, end="")
                    break

    @staticmethod
    def _print_entire_file(path: str) -> None:
        with open(path, "r") as file:
            for line in file:
                print(line, end="")

    def update_job_state(self) -> None:
        """Refresh this job's attributes from ``qstat``."""
        self.read_properties_from_qstat()

    def get_exit_status(self) -> int | None:
        """Return the exit status of the job, or ``None`` if not yet finished."""
        qstat_output = self._run_qstat_to_get_full_job_attributes()
        qstat_dict = self._convert_qstat_output_to_a_dictionary(qstat_output)
        exit_status = qstat_dict.get("Exit_status")
        return int(exit_status) if exit_status is not None else None

    def _this_job_was_launched_from_fake_pbs(self) -> bool:
        return "FakePBS" in self.id

    def _job_is_still_running_or_queued(self) -> bool:
        self.update_job_state()
        return self.state in ("Q", "R")

    def _run_qstat_to_get_full_job_attributes(self) -> list[str]:
        result = subprocess.run(
            ["qstat", "-xf", str(self.id)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,  # Disable automatic decoding
        )
        return result.stdout.decode("utf-8", errors="replace").split("\n")

    @staticmethod
    def _is_a_known_job(qstat_output: list[str]) -> bool:
        return not any("Unknown Job Id" in line for line in qstat_output)

    def _parse_attributes_from_qstat_output(self, qstat_output: list[str]) -> None:
        qstat_dict = self._convert_qstat_output_to_a_dictionary(qstat_output)

        self.name = qstat_dict["Job_Name"]
        self.queue = qstat_dict["queue"]
        self.state = qstat_dict["job_state"]
        self.workdir = self._parse_workdir(qstat_dict)

        select = qstat_dict["Resource_List.select"]
        self.model = select.split("model=")[-1] if "model=" in select else ""
        self.requested_number_of_nodes = int(select.split(":")[0])
        self.ncpus_per_node = int(select.split("ncpus=")[-1].split(":")[0])

        exit_status = qstat_dict.get("Exit_status")
        self.exit_status = int(exit_status) if exit_status is not None else None

        self.walltime_requested = self._convert_walltime_to_seconds(
            qstat_dict["Resource_List.walltime"]
        )
        self.mtime_raw = qstat_dict.get("mtime", "")
        self.mtime = self._parse_mtime(self.mtime_raw)

        if self.state != "Q":
            self.hostname = qstat_dict["exec_host"].split("/")[0]
            walltime_used = qstat_dict.get("resources_used.walltime")
            if walltime_used is not None:
                self.walltime_used = self._convert_walltime_to_seconds(walltime_used)
                self.walltime_remaining = self.walltime_requested - self.walltime_used
            else:
                self.walltime_used = None
                self.walltime_remaining = None

    @staticmethod
    def _convert_walltime_to_seconds(walltime: str) -> int:
        hours, minutes, seconds = walltime.split(":")
        return 3600 * int(hours) + 60 * int(minutes) + int(seconds)

    @staticmethod
    def _parse_mtime(mtime_str: str) -> datetime | None:
        """
        Parse the ``mtime`` field from qstat.

        Two formats are seen in the wild:
        - Newer PBS: ``"Thu Oct 24 12:26:32 2024"``
        - Older PBS: ``"1649348653 (Thu Apr 07 12:24:13 EDT 2022)"`` — leading epoch.
        """
        if not mtime_str:
            return None

        # Older format: leading unix timestamp.
        first_token = mtime_str.split(None, 1)[0]
        if first_token.isdigit():
            try:
                return datetime.fromtimestamp(int(first_token))
            except (ValueError, OSError):
                pass

        # Newer format: ctime-like string.
        try:
            return datetime.strptime(mtime_str, "%a %b %d %H:%M:%S %Y")
        except ValueError:
            return None

    def _set_empty_attributes(self) -> None:
        self.name = ""
        self.model = ""
        self.queue = ""
        self.state = ""
        self.workdir = ""
        self.requested_number_of_nodes = 0
        self.ncpus_per_node = 0
        self.exit_status = None
        self.walltime_requested = None
        self.walltime_used = None
        self.walltime_remaining = None
        self.hostname = ""
        self.mtime_raw = ""
        self.mtime = None

    @staticmethod
    def _parse_workdir(qstat_dict: dict) -> str:
        return qstat_dict["Variable_List"].split("PBS_O_WORKDIR=")[-1].split(",")[0]

    def _convert_qstat_output_to_a_dictionary(self, qstat_output: list[str]) -> dict:
        qstat_dict: dict[str, str] = {}
        current_key: str | None = None

        for line in qstat_output[1:]:
            if not line:
                continue

            if self._is_a_continued_qstat_line(line):
                if current_key is not None:
                    qstat_dict[current_key] += line[1:].strip()
            else:
                key, _, value = line.partition("=")
                current_key = key.strip()
                qstat_dict[current_key] = value.strip()

        return qstat_dict

    @staticmethod
    def _is_a_continued_qstat_line(line: str) -> bool:
        return line.startswith("\t")

    @classmethod
    def from_ids_bulk(cls, ids: list[str]) -> list["PBSJob"]:
        if not ids:
            return []
        result = subprocess.run(
            ["qstat", "-fx", *ids],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False,
        )
        # qstat -f output has "Job Id: <id>" section headers; split on those
        jobs = []
        current_lines: list[str] = []
        current_id: str | None = None
        for line in result.stdout.splitlines():
            if line.startswith("Job Id:"):
                if current_id is not None:
                    job = cls.__new__(cls)          # skip __init__/qstat
                    job.id = current_id
                    job._set_empty_attributes()
                    job._parse_attributes_from_qstat_output(["Job Id: " + current_id] + current_lines)
                    jobs.append(job)
                current_id = line.split(":", 1)[1].strip()
                current_lines = []
            else:
                current_lines.append(line)
        if current_id is not None:
            job = cls.__new__(cls)
            job.id = current_id
            job._set_empty_attributes()
            job._parse_attributes_from_qstat_output(["Job Id: " + current_id] + current_lines)
            jobs.append(job)
        return jobs

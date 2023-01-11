import time
import os
from typing import List, Union


class PBSJob:
    def __init__(self, id: str):
        """
        A class for querying information and managing a particular submitted
        pbs job. For the id number in the constructor, the qstat command will
        be used to populate the attributes of the job.

        Parameters
        ----------
        id:
            The id of the PBS job
        """

        #: The ID of the PBS job
        self.id: str = id

        #: The name of the job
        self.name: str = ''

        #: The model attribute on the select line from the job submission
        self.model: str = ''

        #: The number of resources on the select line
        self.requested_number_of_nodes: int = 0

        #: The number of cpus for node
        self.ncpus_per_node = 0

        #: The queue which this job was submitted to
        self.queue: str = ''

        #: Whether the job is queued, running, or finished
        self.state: str = ''

        #: The value of $PBS_O_WORKDIR
        self.workdir: str = ''

        #: The exit status of the pbs job
        self.exit_status: int = None

        self.read_properties_from_qstat()

    def read_properties_from_qstat(self):
        """
        Use qstat to get the current attributes of this job
        """
        if 'FakePBS' in self.id:
            self._read_properties_from_fake_pbs_launcher_job()
        else:
            self._read_properties_of_real_pbs_job()

    def _read_properties_of_real_pbs_job(self):
        qstat_output = self._run_qstat_to_get_full_job_attributes()
        if self._is_a_known_job(qstat_output):
            self._parse_attributes_from_qstat_output(qstat_output)
        else:
            self._set_empty_attributes()

    def _read_properties_from_fake_pbs_launcher_job(self):
        self.exit_status = int(self.id.split('.')[-1])

    def qdel(self, echo_command: bool = True) -> str:
        """
        Call qdel to delete this job

        Parameters
        ----------
        echo_command:
            Whether to print the command before running it

        Returns
        -------
        command_output: str
            The output of the shell command
        """
        command = f'qdel {self.id}'
        if echo_command:
            print(command)
        return os.popen(command).read()

    def tail_file_until_job_is_finished(self, file_to_tail: str):
        if self._this_job_was_launched_from_fake_pbs():
            # cat the file
            with open(file_to_tail, "r") as file:
                for line in file:
                    print(line)
        else:
            # touch the file first
            if not os.path.exists(file_to_tail):
                open(file_to_tail, 'w').close()

            with open(file_to_tail, "r") as file:
                for line in file:
                    print(line)
                while True:
                    line = file.readline()
                    if line:
                        print(line)
                    else:
                        # Sleep for a bit to avoid wasting resources
                        time.sleep(0.1)
                        if self._job_is_still_running_or_queued():
                            continue
                        else:
                            for line in file:
                                print(line)
                            break

    def update_job_state(self) -> str:
        """
        Get the job's status after it has been submitted.
        Returns the entry of job_state in the qstat information, e.g.,
        'Q', 'R', 'F', 'H', etc.

        """
        self.read_properties_from_qstat()

    def get_exit_status(self) -> int:
        qstat_output = self._run_qstat_to_get_full_job_attributes()
        qstat_dict = self._convert_qstat_output_to_a_dictionary(qstat_output)
        return qstat_dict.get("Exit_status")

    def _this_job_was_launched_from_fake_pbs(self):
        return 'FakePBS' in self.id

    def _job_is_still_running_or_queued(self):
        self.update_job_state()
        if self.state == "Q" or self.state == "R":
            return True
        else:
            return False

    def _run_qstat_to_get_full_job_attributes(self) -> Union[List[str], str]:
        return os.popen(f'qstat -xf {self.id}').read().split('\n')

    def _is_a_known_job(self, qstat_output):
        return not 'Unknown Job Id' in qstat_output

    def _parse_attributes_from_qstat_output(self, qstat_output: List[str]):
        qstat_dict = self._convert_qstat_output_to_a_dictionary(qstat_output)

        self.name: str = qstat_dict['Job_Name']
        self.queue: str = qstat_dict['queue']
        self.state: str = qstat_dict['job_state']
        self.workdir = self._parse_workdir(qstat_dict)

        if 'model' in qstat_dict['Resource_List.select']:
            self.model = qstat_dict['Resource_List.select'].split('model=')[-1]
        else:
            self.model = ''
        self.requested_number_of_nodes = int(qstat_dict['Resource_List.select'].split(':')[0])
        self.ncpus_per_node = int(
            qstat_dict['Resource_List.select'].split('ncpus=')[-1].split(':')[0])

        self.exit_status: int = qstat_dict.get('Exit_status')

    def _set_empty_attributes(self):
        self.name = ''
        self.model = ''
        self.queue = ''
        self.state = ''
        self.workdir = ''
        self.requested_number_of_nodes = 0
        self.ncpus_per_node = 0
        self.exit_status = None

    def _parse_workdir(self, qstat_dict: dict) -> str:
        return qstat_dict['Variable_List'].split('PBS_O_WORKDIR=')[-1].split(',')[0]

    def _convert_qstat_output_to_a_dictionary(self, qstat_output: List[str]) -> dict:
        qstat_dict = {}
        for line in qstat_output:
            if '=' in line:
                split_line = line.split('=', 1)
                key = split_line[0].strip()
                value = split_line[1].strip()
                qstat_dict[key] = value
        return qstat_dict

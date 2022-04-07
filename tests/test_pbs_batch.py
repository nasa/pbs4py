import os
import pytest
from typing import List

from pbs4py import PBSBatch, BatchJob
from pbs4py.directory_utils import cd

test_directory = os.path.dirname(os.path.abspath(__file__))


class MockJob(BatchJob):
    def __init__(self, name: str, body: List[str]):
        super().__init__(name, body)
        self.state_check_count = 0

    def get_pbs_job_state(self) -> str:
        self.state_check_count += 1
        if self.state_check_count == 3:
            return 'F'
        elif self.state_check_count == 2:
            return 'R'
        elif self.state_check_count == 1:
            return 'Q'


class MockPBS:
    def __init__(self):
        self.id_counter = -1

    def launch(self, job_name, job_body, blocking=True):
        self.id_counter += 1
        return str(self.id_counter)


@pytest.fixture
def batch():
    jobs = [MockJob('job0', ['ls']),
            MockJob('job1', ['echo Hello World!']),
            MockJob('job2', ['pwd'])]
    return PBSBatch(MockPBS(), jobs, use_separate_directories=False)


def test_create_directories(batch: PBSBatch):
    expected_dirs = ['job0', 'job1', 'job2']

    with cd(test_directory):
        for d in expected_dirs:
            assert not os.path.exists(d)

        batch.create_directories()

        for d in expected_dirs:
            assert os.path.exists(d)
            os.system(f'rm -r {d}')


def test_launch(batch: PBSBatch):
    batch.launch_all_jobs()
    for i, job in enumerate(batch.jobs):
        assert str(i) == job.id


def test_wait_for_all_jobs_to_finish(batch: PBSBatch):
    batch.wait_for_all_jobs_to_finish(check_frequency_in_secs=0.1)

    for job in batch.jobs:
        assert job.state_check_count == 3


def test_all_jobs_submitted(batch: PBSBatch):
    assert batch._all_jobs_submitted(3)
    assert not batch._all_jobs_submitted(2)

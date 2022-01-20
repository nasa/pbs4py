import pytest
import os

from pbs4py import Job

test_directory = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def job():
    return Job('job_test', ['ls'])


def test_job_directory_context_manager(job: Job):
    cwd = os.getcwd()
    os.chdir(test_directory)

    test_file = 'empty_file'
    assert not os.path.exists(test_file)
    with job:
        assert os.path.exists(test_file)

    assert not os.path.exists(test_file)

    os.chdir(cwd)


def test_job_state_before_launch(job: Job):
    assert job.get_pbs_job_state() == ''

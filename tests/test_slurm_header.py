import os
import pytest
from pbs4py.slurm import SLURM

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


@pytest.fixture
def slurm_header_test():
    queue_name = 'queue'
    ncpus_per_node = 5
    queue_node_limit = 20
    time = 12
    hashbang = '#!/usr/bin/tcsh'
    pbs_header_test = SLURM(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
                            queue_node_limit=queue_node_limit, time=time,
                            profile_filename=test_profile)
    pbs_header_test.hashbang = hashbang
    pbs_header_test.requested_number_of_nodes = 4
    return pbs_header_test


def test_job_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_job_line_of_header("dog")
    assert line == "#SBATCH --job-name=dog"


def test_queue_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_queue_line_of_header()
    assert line == "#SBATCH --partition=queue"


def test_nodes_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_nodes_line_of_header()
    assert line == "#SBATCH --nodes=4"


def test_tasks_per_node_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_tasks_per_node_line_of_header()
    assert line == "#SBATCH --ntasks-per-node=5"


def test_walltime_line(slurm_header_test: SLURM):
    slurm_header_test.time = 16
    line = slurm_header_test._create_walltime_line_of_header()
    assert line == "#SBATCH --time=16:00:00"


def test_log_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_log_name_line_of_header("dog")
    assert line == "#SBATCH --output=qlog_dog"


def test_error_log_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_header_line_to_error_output("dog")
    assert line == "#SBATCH --error=err_dog"


def test_not_rerunnable_line(slurm_header_test: SLURM):
    line = slurm_header_test._create_header_line_to_set_that_job_is_not_rerunnable()
    assert line == "#SBATCH --no-requeue"


def test_account_line(slurm_header_test: SLURM):
    lines = slurm_header_test._create_account_header_line()
    assert len(lines) == 0

    slurm_header_test.account = "a123"
    lines = slurm_header_test._create_account_header_line()
    assert len(lines) == 1
    assert lines[0] == "#SBATCH --account=a123"


def test_array_range_header_line(slurm_header_test: SLURM):
    lines = slurm_header_test._create_array_range_header_line()
    assert len(lines) == 0

    slurm_header_test.array_range = '1-2'
    lines = slurm_header_test._create_array_range_header_line()
    assert len(lines) == 1
    assert lines[0] == "#SBATCH --array=1-2"


def test_mail_options_lines(slurm_header_test: SLURM):
    lines = slurm_header_test._create_mail_options_header_lines()
    assert len(lines) == 0

    slurm_header_test.mail_options = "BEGIN"
    slurm_header_test.mail_list = "test@nasa.gov"
    lines = slurm_header_test._create_mail_options_header_lines()
    assert len(lines) == 2
    assert lines[0] == "#SBATCH --mail-type=BEGIN"
    assert lines[1] == "#SBATCH --mail-user=test@nasa.gov"


def test_dependency_lines(slurm_header_test: SLURM):
    lines = slurm_header_test._create_job_dependencies_header_line(None)
    assert len(lines) == 0

    lines = slurm_header_test._create_job_dependencies_header_line("a123")
    assert len(lines) == 1
    assert lines[0] == "#SBATCH --dependency=afterok:a123"


def test_nodelist_line(slurm_header_test: SLURM):
    lines = slurm_header_test._create_nodelist_header_line()
    assert len(lines) == 0

    slurm_header_test.nodelist = '1,2,3,4'
    lines = slurm_header_test._create_nodelist_header_line()
    assert len(lines) == 1
    assert lines[0] == "#SBATCH --nodelist=1,2,3,4"

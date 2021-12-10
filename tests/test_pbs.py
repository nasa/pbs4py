import os
import pytest
import filecmp
from pbs4py import PBS

test_directory = os.path.dirname(os.path.abspath(__file__))


def test_profile_file_checking():
    actual_file_location = "~"

    pbs = PBS.k4()
    pbs.profile_filename = actual_file_location

    assert pbs.profile_filename == actual_file_location

    nonexistant_file = "i_am_not_a_file.xyz"
    with pytest.raises(FileNotFoundError):
        pbs.profile_filename = nonexistant_file


@pytest.fixture
def pbs_header_test():
    queue_name = 'queue'
    ncpus_per_node = 5
    queue_node_limit = 10
    time = 24
    hashbang = '#!/usr/bin/tcsh'
    pbs_header_test = PBS(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
                          queue_node_limit=queue_node_limit, time=time)
    pbs_header_test.hashbang = hashbang
    pbs_header_test.requested_number_of_nodes = 2
    return pbs_header_test


def test_pbs_header(pbs_header_test):
    job_name = 'test_job'
    header = pbs_header_test._create_pbs_header(job_name)

    assert header[0] == "#!/usr/bin/tcsh"
    assert header[1] == "#PBS -N test_job"
    assert header[2] == "#PBS -q queue"
    assert header[3] == "#PBS -l select=2:ncpus=5:mpiprocs=5"
    assert header[4] == "#PBS -l walltime=24:00:00"
    assert header[5] == "#PBS -o test_job_pbs.log"
    assert header[6] == "#PBS -j oe"
    assert header[7] == "#PBS -r n"
    assert len(header) == 8


def test_pbs_header_email_option(pbs_header_test: PBS):
    pbs_header_test.mail_options = 'be'
    pbs_header_test.mail_list = 'kevin@nasa.gov'
    job_name = 'test_job'
    header = pbs_header_test._create_pbs_header(job_name)

    assert header[-2] == '#PBS -m be'
    assert header[-1] == '#PBS -M kevin@nasa.gov'


def test_pbs_header_group_list(pbs_header_test: PBS):
    pbs_header_test.group_list = 'n1337'
    job_name = 'test_job'
    header = pbs_header_test._create_pbs_header(job_name)

    assert header[2] == '#PBS -W group_list=n1337'


def test_pbs_header_with_model_defined(pbs_header_test):
    pbs_header_test.model = 'bro'
    job_name = 'test_job'
    header = pbs_header_test._create_pbs_header(job_name)
    for header_line in header:
        if '-l select' in header_line:
            assert header_line == "#PBS -l select=2:ncpus=5:mpiprocs=5:model=bro"


def test_pbs_header_with_group_name_defined(pbs_header_test):
    pbs_header_test.group_list = 'n1337'
    job_name = 'test_job'
    header = pbs_header_test._create_pbs_header(job_name)
    assert header[2] == "#PBS -W group_list=n1337"


def test_create_mpi_command_openmpi():
    pbs = PBS()
    pbs.ncpus_per_node = 30
    pbs.mpiexec = 'mpirun'
    dummy_command = 'foo'
    output_root_name = 'dog'

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name)
    expected_command = 'mpirun foo > dog.out 2>&1'
    assert mpi_command == expected_command

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name, openmp_threads=5)
    expected_command = 'OMP_NUM_THREADS=5 OMP_PLACES=cores OMP_PROC_BIND=close mpirun -np 6 foo > dog.out 2>&1'
    assert mpi_command == expected_command


def test_create_mpi_command_mpt():
    pbs = PBS()
    pbs.ncpus_per_node = 20
    pbs.mpiexec = 'mpiexec_mpt'
    dummy_command = 'foo'
    output_root_name = 'dog'

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name)
    expected_command = 'mpiexec_mpt foo > dog.out 2>&1'
    assert mpi_command == expected_command

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name, openmp_threads=5)
    expected_command = 'OMP_NUM_THREADS=5 mpiexec_mpt -np 4 omplace -c "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19" -nt 5 -vv foo > dog.out 2>&1'
    assert mpi_command == expected_command


def test_write_pbs_file():
    golden_file = f'{test_directory}/pbs_test_files/golden0.pbs'
    queue_name = 'queue'
    ncpus_per_node = 5
    queue_node_limit = 10
    time = 24
    hashbang = '#!/usr/bin/bash'
    pbs = PBS(queue_name=queue_name, ncpus_per_node=ncpus_per_node,
              queue_node_limit=queue_node_limit, time=time)
    pbs.hashbang = hashbang

    job_name = 'test_job'
    job_body = ['command1', 'command2']
    pbs_file = f'{test_directory}/test_output_files/test.pbs'
    pbs.write_pbs_file(pbs_file, job_name, job_body)

    assert filecmp.cmp(pbs_file, golden_file)

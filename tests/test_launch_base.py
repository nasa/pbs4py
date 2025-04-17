import os
import pytest
from pbs4py.launcher_base import Launcher

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


@pytest.fixture
def launcher():
    return Launcher(1, 1, 1, 1, test_profile, 1)


def test_profile_file_checking():
    real_file = 'pbs4py_unit_test_dummy.txt'
    os.system(f'touch {real_file}')

    launcher = Launcher(1, 1, 1, 1, real_file, 1)
    assert launcher.profile_filename == real_file
    os.system(f'rm {real_file}')

    nonexistant_file = "i_am_not_a_file.xyz"
    with pytest.raises(FileNotFoundError):
        launcher.profile_filename = nonexistant_file


def test_output_redirection(launcher: Launcher):
    launcher.shell = 'tcsh'
    assert launcher._redirect_shell_output('dog.out') == '>& dog.out'

    launcher.shell = 'bash'
    assert launcher._redirect_shell_output('dog.out') == '&> dog.out'

    launcher.tee_output = True
    assert launcher._redirect_shell_output('dog.out') == '2>&1 | tee dog.out'


def test_create_mpi_command_openmpi(launcher: Launcher):
    launcher.ncpus_per_node = 30
    launcher.mpiexec = 'mpirun'
    dummy_command = 'foo'
    output_root_name = 'dog'

    if not launcher._using_mpt():
        mpi_command = launcher.create_mpi_command(dummy_command, output_root_name)
        expected_command = 'mpirun foo &> dog.out'
        assert mpi_command == expected_command

        mpi_command = launcher.create_mpi_command(dummy_command, output_root_name, openmp_threads=5)
        expected_command = 'OMP_NUM_THREADS=5 OMP_PLACES=cores OMP_PROC_BIND=close mpirun --npernode 6 foo &> dog.out'
        assert mpi_command == expected_command

        mpi_command = launcher.create_mpi_command(dummy_command, output_root_name, ranks_per_node=3)
        expected_command = 'mpirun --npernode 3 foo &> dog.out'
        assert mpi_command == expected_command


def test_mpiprocs(launcher: Launcher):
    launcher.ncpus_per_node = 20
    assert launcher.mpiprocs_per_node == 20

    launcher.ncpus_per_node = 40
    assert launcher.mpiprocs_per_node == 40

    launcher.mpiprocs_per_node = 4
    assert launcher.mpiprocs_per_node == 4
    assert launcher.ncpus_per_node == 40

import os
import pytest
from pbs4py import PBS

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


def test_profile_file_checking():
    real_file = 'pbs4py_unit_test_dummy.txt'
    os.system(f'touch {real_file}')

    pbs = PBS.k4(profile_file=real_file)
    assert pbs.profile_filename == real_file
    os.system(f'rm {real_file}')

    nonexistant_file = "i_am_not_a_file.xyz"
    with pytest.raises(FileNotFoundError):
        pbs.profile_filename = nonexistant_file


def test_output_redirection():
    pbs = PBS(profile_file=test_profile)
    pbs.shell = 'bash'
    assert pbs._redirect_shell_output('dog.out') == '&> dog.out'

    pbs.shell = 'tcsh'
    assert pbs._redirect_shell_output('dog.out') == '>& dog.out'


def test_create_mpi_command_openmpi():
    pbs = PBS(profile_file=test_profile)
    pbs.ncpus_per_node = 30
    pbs.mpiexec = 'mpirun'
    dummy_command = 'foo'
    output_root_name = 'dog'

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name)
    expected_command = 'mpirun foo &> dog.out'
    assert mpi_command == expected_command

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name, openmp_threads=5)
    expected_command = 'OMP_NUM_THREADS=5 OMP_PLACES=cores OMP_PROC_BIND=close mpirun -np 6 foo &> dog.out'
    assert mpi_command == expected_command


def test_create_mpi_command_mpt():
    pbs = PBS(profile_file=test_profile)
    pbs.ncpus_per_node = 20
    pbs.mpiexec = 'mpiexec_mpt'
    dummy_command = 'foo'
    output_root_name = 'dog'

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name)
    expected_command = 'mpiexec_mpt foo &> dog.out'
    assert mpi_command == expected_command

    mpi_command = pbs.create_mpi_command(dummy_command, output_root_name, openmp_threads=5)
    expected_command = 'OMP_NUM_THREADS=5 mpiexec_mpt -np 4 omplace -c "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19" -nt 5 -vv foo &> dog.out'
    assert mpi_command == expected_command


def test_k3_class_method():
    k3 = PBS.k3(profile_file=test_profile)
    assert k3.queue_name == 'K3-route'
    assert k3.ncpus_per_node == 16
    assert k3.queue_node_limit == 40


def test_k3a_class_method():
    k3a = PBS.k3a(profile_file=test_profile)
    assert k3a.queue_name == 'K3a-route'
    assert k3a.ncpus_per_node == 16
    assert k3a.queue_node_limit == 25


def test_k4_class_method():
    k4 = PBS.k4(profile_file=test_profile)
    assert k4.queue_name == 'K4-route'
    assert k4.ncpus_per_node == 40
    assert k4.queue_node_limit == 16


def test_nas_cascadelake_class_method():
    nas = PBS.nas('n1337', 'cas', profile_file=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 40
    assert nas.model == 'cas_ait'


def test_nas_skylake_class_method():
    nas = PBS.nas('n1337', 'skylake', profile_file=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 40
    assert nas.model == 'sky_ele'


def test_nas_broadwell_class_method():
    nas = PBS.nas('n1337', 'bro', profile_file=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 28
    assert nas.model == 'bro'


def test_nas_haswell_class_method():
    nas = PBS.nas('n1337', 'has', profile_file=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 24
    assert nas.model == 'has'


def test_nas_ivybridge_class_method():
    nas = PBS.nas('n1337', 'ivy', profile_file=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 20
    assert nas.model == 'ivy'


def test_nas_sandybridge_class_method():
    nas = PBS.nas('n1337', 'san', profile_file=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 20
    assert nas.model == 'san'


def test_nas_class_method_with_bad_queue_name():
    with pytest.raises(ValueError):
        nas = PBS.nas('n1337', 'not_a_queue', profile_file=test_profile)

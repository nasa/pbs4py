import os
import pytest
from pbs4py import PBS

test_directory = os.path.dirname(os.path.abspath(__file__))
test_profile = f'{test_directory}/testing_bashrc'


def test_k3a_class_method():
    k3a = PBS.k3a(profile_filename=test_profile)
    assert k3a.queue_name == 'K3a-route'
    assert k3a.ncpus_per_node == 16
    assert k3a.queue_node_limit == 25


def test_k3b_class_method():
    k3 = PBS.k3b(profile_filename=test_profile)
    assert k3.queue_name == 'K3b-route'
    assert k3.ncpus_per_node == 28
    assert k3.queue_node_limit == 74


def test_k3c_class_method():
    k3 = PBS.k3c(profile_filename=test_profile)
    assert k3.queue_name == 'K3c-route'
    assert k3.ncpus_per_node == 28
    assert k3.queue_node_limit == 74


def test_k4_class_method():
    k4 = PBS.k4(profile_filename=test_profile)
    assert k4.queue_name == 'K4-route'
    assert k4.ncpus_per_node == 40
    assert k4.queue_node_limit == 16


def test_k4_v100_class_method():
    k4v100 = PBS.k4_v100(profile_filename=test_profile)
    assert k4v100.queue_name == 'K4-V100'
    assert k4v100.ncpus_per_node == 4
    assert k4v100.queue_node_limit == 4


def test_k5_a100_40_class_method():
    k5 = PBS.k5_a100_40(profile_filename=test_profile)
    assert k5.queue_name == 'K5-A100-40'
    assert k5.ncpus_per_node == 8
    assert k5.queue_node_limit == 2


def test_k5_a100_80_class_method():
    k5 = PBS.k5_a100_80(profile_filename=test_profile)
    assert k5.queue_name == 'K5-A100-80'
    assert k5.ncpus_per_node == 8
    assert k5.queue_node_limit == 2


def test_nas_cascadelake_class_method():
    nas = PBS.nas('n1337', 'cas', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 40
    assert nas.model == 'cas_ait'


def test_nas_skylake_class_method():
    nas = PBS.nas('n1337', 'skylake', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 40
    assert nas.model == 'sky_ele'


def test_nas_broadwell_class_method():
    nas = PBS.nas('n1337', 'bro', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 28
    assert nas.model == 'bro'


def test_nas_haswell_class_method():
    nas = PBS.nas('n1337', 'has', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 24
    assert nas.model == 'has'


def test_nas_ivybridge_class_method():
    nas = PBS.nas('n1337', 'ivy', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 20
    assert nas.model == 'ivy'


def test_nas_sandybridge_class_method():
    nas = PBS.nas('n1337', 'san', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 16
    assert nas.model == 'san'


def test_nas_mil_class_method():
    nas = PBS.nas('n1337', 'mil', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 128
    assert nas.model == 'mil_ait'


def test_nas_rom_class_method():
    nas = PBS.nas('n1337', 'rom', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 128
    assert nas.model == 'rom_ait'


def test_nas_mil_a100_class_method():
    nas = PBS.nas('n1337', 'mil_a100', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 64
    assert nas.ngpus_per_node == 4
    assert nas.mem == '500G'
    assert nas.model == 'mil_a100'


def test_nas_sky_gpu_class_method():
    nas = PBS.nas('n1337', 'sky_gpu', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 36
    assert nas.ngpus_per_node == 4
    assert nas.mem == '200G'
    assert nas.model == 'sky_gpu'


def test_nas_cas_gpu_class_method():
    nas = PBS.nas('n1337', 'cas_gpu', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 48
    assert nas.ngpus_per_node == 4
    assert nas.mem == '200G'
    assert nas.model == 'cas_gpu'


def test_nas_rom_gpu_class_method():
    nas = PBS.nas('n1337', 'rom_gpu', profile_filename=test_profile)
    assert nas.group_list == 'n1337'
    assert nas.ncpus_per_node == 128
    assert nas.ngpus_per_node == 8
    assert nas.mem == '700G'
    assert nas.model == 'rom_gpu'


def test_nas_class_method_with_bad_queue_name():
    with pytest.raises(ValueError):
        PBS.nas('n1337', 'not_a_queue', profile_filename=test_profile)


def test_cf1_class_method():
    cf1 = PBS.cf1('acct', profile_filename=test_profile)
    assert cf1.queue_name == "normal"
    assert cf1.group_list == 'acct'
    assert cf1.workdir_env_variable == "$SLURM_SUBMIT_DIR"
    assert cf1.queue_node_limit == 30
    assert cf1.ncpus_per_node == 64

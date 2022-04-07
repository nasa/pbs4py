import pytest
from pbs4py.job import PBSJob


class FakeKJob(PBSJob):
    def _run_qstat_to_get_full_job_attributes(self):
        stand_in_output = [
            "    Job: 2493765.pbssrv2", "Job_Name = sample0", "Job_Owner = kejacob1@k4-li1-ib0",
            "resources_used.cpupercent = 100", "resources_used.cput = 00:00:02",
            "resources_used.mem = 1528kb", "resources_used.ncpus = 16",
            "resources_used.vmem = 15936kb", "resources_used.walltime = 00:00:02", "job_state = F",
            "queue = K3a-standard", "server = pbssrv2", "Checkpoint = u",
            "ctime = 1649348639 (Thu Apr 07 12:23:59 EDT 2022)",
            "Error_Path = k4-li1-ib0:/lustre3/hpnobackup2/kejacob1/projects/cad_to_solution/pbs4py/examples/batch_with_job_limit/sample0/sample0.e2493765",
            "exec_host = k3ar5n1/0*16", "exec_vnode = (k3ar5n1:ncpus=16)", "Hold_Types = n",
            "Join_Path = oe", "Keep_Files = n", "Mail_Points = a",
            "mtime = 1649348653 (Thu Apr 07 12:24:13 EDT 2022)",
            "Output_Path = k4-li1-ib0.ccf-beowulf.larc.nasa.gov:/lustre3/hpnobackup2/kejacob1/projects/cad_to_solution/pbs4py/examples/batch_with_job_limit/sample0/sample0_pbs.log",
            "Priority = 0", "qtime = 1649348639 (Thu Apr 07 12:23:59 EDT 2022)",
            "Rerunable = False", "Resource_List.mem = 31gb", "Resource_List.mpiprocs = 16",
            "Resource_List.ncpus = 16", "Resource_List.nodect = 1",
            "Resource_List.nodegroup = K3a-open", "Resource_List.place = scatter:excl",
            "Resource_List.select = 1:ncpus=16:mpiprocs=16", "Resource_List.walltime = 72:00:00",
            "stime = 1649348640 (Thu Apr 07 12:24:00 EDT 2022)", "session_id = 22053",
            "jobdir = /u/kejacob1", "substate = 92",
            "Variable_List = PBS_O_SYSTEM=Linux,PBS_O_SHELL=/bin/bash,PBS_O_HOME=/u/kejacob1,PBS_O_HOST=k4-li1-ib0,PBS_O_LOGNAME=kejacob1,PBS_O_WORKDIR=/lustre3/hpnobackup2/kejacob1/projects/cad_to_solution/pbs4py/examples/batch_with_job_limit/sample0,PBS_O_LANG=C,PBS_O_PATH=/usr/local/pkgs-viz/cuda_11.0.167/bin:/usr/local/pkgs-viz/cuda_11.0.167/nvvm/bin:/u/kejacob1/bin/gdb/bin:/u/kejacob1/.local/bin:/u/kejacob1/bin:/usr/local/pkgs-modules/cmake_3.6.3/bin:/usr/local/pkgs-modules/intel_2018.0.033/inspector/bin64:/usr/local/pkgs-modules/intel_2018.0.033/advisor/bin64:/usr/local/pkgs-modules/intel_2018.0.033/compilers_and_libraries_2018.3.222/linux/bin/intel64:/usr/local/pkgs-modules/intel_2018.0.033/vtune_amplifier/bin64:/usr/local/pkgs-modules/intel_2018.0.033/compilers_and_libraries_2018.3.222/debugger_2018/gdb/intel64/bin:/usr/local/pkgs-modules/openmpi_3.0.1_intel_2018/bin:/usr/local/pkgs-modules/gcc_6.2.0/bin:/usr/local/pkgs-modules/tecplot360ex-2018R1/bin:/usr/local/pkgs-modules/tecplot360ex-2018R2/360ex_2018r2/bin:/lustre3/hpnobackup2/kejacob1/projects/post2/post2_env/bin:/usr/local/pkgs-modules/Python_3.7.1/bin:/usr/local/pkgs/modules_4.2.4/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin,PBS_O_QUEUE=K3a-route,PBS_O_MAIL=/var/spool/mail/kejacob1",
            "comment = Job run at Thu Apr 07 at 12:24 on (k3ar5n1:ncpus=16) and finished",
            "etime = 1649348639 (Thu Apr 07 12:23:59 EDT 2022)", "run_count = 1",
            "eligible_time = 00:00:00", "Stageout_status = 1", "Exit_status = 0",
            "Submit_arguments = <jsdl-hpcpa:Argument>sample0.pbs</jsdl-hpcpa:Argument>",
            "history_timestamp = 1649348653", "project = _pbs_project_default"]
        return stand_in_output


class FakeNASJob(PBSJob):
    def _run_qstat_to_get_full_job_attributes(self):
        stand_in_output = [
            "Job: 13198744.pbspl1.nas.nasa.gov",
            "    Job_Name = C006ste",
            "    Job_Owner = kejacob1@pfe23.nas.nasa.gov",
            "    job_state = Q",
            "    queue = devel",
            "    server = pbspl1.nas.nasa.gov",
            "    Checkpoint = u",
            "    ctime = 1649355753 (Thu Apr 07 11:22:33 PDT 2022)",
            "    Error_Path = pfe23.nas.nasa.gov:/nobackup/kejacob1/projects/sfe/support/C006ste.e13198744",
            "    group_list = c1454",
            "    Hold_Types = n",
            "    Join_Path = oe",
            "    Keep_Files = n",
            "    Mail_Points = a",
            "    mtime = 1649355753 (Thu Apr 07 11:22:33 PDT 2022)",
            "    Output_Path = pfe23.nas.nasa.gov:/nobackup/kejacob1/projects/sfe/support/C006ste.o13198744",
            "    Priority = 0",
            "    qtime = 1649355753 (Thu Apr 07 11:22:33 PDT 2022)",
            "    Rerunable = False",
            "    Resource_List.mpiprocs = 640",
            "    Resource_List.ncpus = 640",
            "    Resource_List.nobackupp2 = 1",
            "    Resource_List.nodect = 16",
            "    Resource_List.place = scatter:excl",
            "    Resource_List.select = 16:ncpus=40:mpiprocs=40:model=sky_ele",
            "    Resource_List.walltime = 02:00:00",
            "    schedselect = 16:ncpus=40:mpiprocs=40:model=sky_ele:aoe=toss3:bigmem=False:reboot=free",
            "    substate = 10",
            "    Variable_List = PBS_O_MAIL=/var/mail/kejacob1,PBS_O_PATH=/home1/kejacob1/.local/bin:/home1/kejacob1/bin:/nasa/pkgsrc/toss3/2021Q2/views/python/3.9.5/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/X11R6/bin:/PBS/bin:/usr/sbin:/sbin:/opt/c3/bin:/opt/sgi/sbin:/opt/sgi/bin,PBS_O_HOME=/home1/kejacob1,PBS_O_SHELL=/bin/bash,PBS_O_TZ=PST8PDT,PBS_O_SYSTEM=Linux,PBS_O_LOGNAME=kejacob1,PBS_O_LANG=C,PBS_O_WORKDIR=/nobackup/kejacob1/projects/sfe/support,PBS_O_QUEUE=devel,PBS_O_HOST=pfe23.nas.nasa.gov",
            "    euser = kejacob1",
            "    egroup = c1454",
            "    queue_type = E",
            "    etime = 1649355753 (Thu Apr 07 11:22:33 PDT 2022)",
            "    eligible_time = 00:00:00",
            "    Submit_arguments = <jsdl-hpcpa:Argument>pfe.pbs</jsdl-hpcpa:Argument>",
            "    project = _pbs_project_default",
            "    Submit_Host = pfe23.nas.nasa.gov"]
        return stand_in_output


class FakeUnknownJob(PBSJob):
    def _run_qstat_to_get_full_job_attributes(self) -> str:
        return 'qstat: Unknown Job Id 123456.pbssrv2'


def test_read_K_properties_from_qstat():
    job = FakeKJob(2493765)

    assert job.id == 2493765
    assert job.name == 'sample0'
    assert job.queue == 'K3a-standard'
    assert job.state == 'F'
    assert job.workdir == '/lustre3/hpnobackup2/kejacob1/projects/cad_to_solution/pbs4py/examples/batch_with_job_limit/sample0'
    assert job.model == ''


def test_read_NAS_properties_from_qstat():
    job = FakeNASJob(13198744)

    assert job.id == 13198744
    assert job.name == 'C006ste'
    assert job.queue == 'devel'
    assert job.state == 'Q'
    assert job.workdir == '/nobackup/kejacob1/projects/sfe/support'
    assert job.model == 'sky_ele'


def test_unknown_job():
    job = FakeUnknownJob(123456)
    assert job.id == 123456
    assert job.name == ''
    assert job.queue == ''
    assert job.state == ''
    assert job.workdir == ''
    assert job.model == ''

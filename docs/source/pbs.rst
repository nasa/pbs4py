.. _pbs_section:

PBS Job Handling
%%%%%%%%%%%%%%%%

The PBS class is a tool to define properties of the PBS set up you want to use,
write pbs scripts, and launch jobs.


PBS stand alone example
=======================

.. code-block :: python

    from pbs4py import PBS

    k4 = PBS.k4(time=48)
    k4.mpiexec = 'mpiexec_mpt'
    k4.requested_number_of_nodes = 5

    fun3d_command = 'nodet_mpi --gamma 1.14'
    fun3d_mpi_command = k4.create_mpi_command(fun3d_command, 'dog')

    pbs_commands = ['echo Start', fun3d_mpi_command, 'echo Done']

    # submit and move on
    k4.launch('test_job', pbs_commands, blocking=False)

    # submit and wait for job to finish before continuing script
    k4.launch('test_job', pbs_commands)

This script will launch pbs jobs with the following test_jobs.pbs script:

.. code-block :: bash

    #!/usr/bin/env bash
    #PBS -N test_job
    #PBS -q K4-route
    #PBS -l select=5:ncpus=40:mpiprocs=40
    #PBS -l walltime=48:00:00
    #PBS -o test_job_pbs.log
    #PBS -j oe
    #PBS -r n


    cd $PBS_O_WORKDIR
    source ~/.bashrc

    echo Start
    mpiexec_mpt nodet_mpi --gamma 1.14 > dog.out 2>&1
    echo Done

PBS Class
=========
The basic queue attributes are set in the constructor, but less common ones can be adjusted by changing the attributes.

.. automodule:: pbs4py.pbs

.. autoclass:: PBS
   :members:

PBS's classmethod constructors
------------------------------
Although not captured by the sphinx's autodocumentation,
the ``PBS`` class has several classmethods that serve as alternate constructors which fill in properties of some NASA HPC systems and queue.


.. code-block :: python

    from pbs4py import PBS

    k4 = PBS.k4(time=48)
    k3 = PBS.k3()
    k3a = PBS.k3a()
    nas = PBS.nas(group_list='n1337', proc_type='skylake', time = 72)

FakePBS Class
*************
Some scripts may be set up with the PBS job handler originally, but you may want to run
the script within an existing PBS job without launching new PBS jobs.
The FakePBS object appears to driving scripts as a standard PBS object,
but directly runs the commands instead of putting them into a PBS job and launching the job.

.. autoclass:: FakePBS
   :members:

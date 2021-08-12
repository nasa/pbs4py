.. _pbs_section:

PBS Job Handling
%%%%%%%%%%%%%%%%

The adaptation process requires automation of creating and submitting PBS jobs.
However each HPC machine's PBS set up is slightly different.
The PBS class is a tool to define properties of the PBS set up you want to use
and then launch jobs.

Note: Nothing about these utility classes is specific to adaptation, so they
could be used for other job scripting.


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
Some adaptation such as 2D problems are small enough to run everything in a single pbs job so that you avoid having to sit in the queue for each operation.
The FakePBS object appears to the adaptation drivers as a standard PBS object, but directly runs the commands instead of putting them into a PBS job and launching the job.

.. autoclass:: FakePBS
   :members:

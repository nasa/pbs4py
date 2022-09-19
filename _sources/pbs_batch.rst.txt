.. _pbs_batch_section:

PBS Job Batch Submission
%%%%%%%%%%%%%%%%%%%%%%%%

The PBSBatch class is a tool launch many jobs simultaneously.

The basic steps are:

1. Instantiating a :class:`~pbs4py.pbs.PBS` that will be used to submit the jobs.
2. Creating a list of :class:`~pbs4py.pbs_batch.BatchJob` objects that hold the name of the job and a list of the commands to run.
3. Setting up the job directory with the appropriate input files.
4. Giving the ``PBS`` object and list of ``BatchJob`` to the :class:`~pbs4py.pbs_batch.PBSBatch` constructor and then calling one of the launch methods.

Setting up the Job Directories
==============================
By default jobs are launched in directories with the same name as the job.
This prevents concurrent jobs in the batch from overwriting each other's output files.

To set up a job, these directories can be created and populated with code like this:

.. code-block :: python

    batch = PBSBatch(pbs,jobs)

    batch.create_directories()
    common_inputs_to_copy = ['fun3d.nml','*.cfg']

    for job in jobs:
        for input in common_inputs_to_copy:
            os.system(f'cp {input} {job.name}')


Launch Methods
==============
The batch jobs can be submitted with two different methods of the :class:`~pbs4py.pbs_batch.PBSBatch` class.

:func:`~pbs4py.pbs_batch.PBSBatch.launch_jobs_with_limit` will launch every job in the list,
but it will only allow a certain number of jobs to be active in the queue system
(queued, running, held) at a time. This would be the preferred launch method if
you have many jobs and don't want to submit 100s of jobs into the queue at a time
as a courtesy to your fellow HPC users.

:func:`~pbs4py.pbs_batch.PBSBatch.launch_all_jobs` will launch every job in the list.
It has an optional argument to wait for the jobs to finish before returning or
returning immediately after all of the jobs are submitted to the queue.

Batch Job Class
===============
.. automodule:: pbs4py.pbs_batch

.. autoclass:: BatchJob
   :members:

PBSBatch Class
==============

.. autoclass:: PBSBatch
   :members:

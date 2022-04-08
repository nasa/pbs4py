.. _pbs_script_section:

Scripts
%%%%%%%

Job Directory Script
====================

.. argparse::
    :ref: pbs4py.scripts.job_dir.arg_parser
    :prog: job_dir

qdir alias to cd to job's directory
-----------------------------------

This script to print the job directory can be used in combination with bash
aliases to create a ``qdir`` alias for moving to the directory a job is running in

.. code-block:: bash

    qdirfun() { cd `job_dir.py $1`;}
    alias qdir=qdirfun

Then in the shell instance you can do ``qdir {job_id}`` to move the job's run directory.

Qdel for User Jobs Script
=========================


.. argparse::
    :ref: pbs4py.scripts.qdel_user_jobs.arg_parser
    :prog: job_dir


Example
-------
The following command would delete the current users jobs that meet these conditions: PBS ids between 1000 and 2400,
in the K3-standard queue, and have ``crm`` in the job name. By default the list of jobs will be
printed to the screen asking the user for confirmation. Add ``--no-confirm`` would skip this step.

.. code-block:: bash

    qdel_user_jobs.py --id_range 1000 2400 --queue K3-standard --name crm

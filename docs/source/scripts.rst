.. _pbs_script_section:

Scripts
%%%%%%%

Job Directory Script
--------------------

.. argparse::
    :ref: pbs4py.scripts.job_dir.arg_parser
    :prog: job_dir

qdir alias to cd to job's directory
===================================

This script to print the job directory can be used in combination with bash
aliases to create a ``qdir`` alias for moving to the directory a job is running in

.. code-block:: bash

    qdirfun() { cd `job_dir.py $1`;}
    alias qdir=qdirfun

Then in the shell instance you can do ``qdir {job_id}`` to move the job's run directory.

Qdel for User Jobs Script
-------------------------


.. argparse::
    :ref: pbs4py.scripts.qdel_user_jobs.arg_parser
    :prog: job_dir

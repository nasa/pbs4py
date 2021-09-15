pbs4py
======

Python scripting for PBS jobs.


.. toctree::
   :maxdepth: 1

   pbs.rst
   pbs_batch.rst

Quick Start
===========

.. code-block :: python

    from pbs4py import PBS

    k4 = PBS.k4(time=48)
    k4.mpiexec = 'mpiexec_mpt'
    k4.requested_number_of_nodes = 2

    fun3d_command = 'nodet_mpi --gamma 1.14'
    fun3d_mpi_command = k4.create_mpi_command(fun3d_command, 'dog')
    pbs_commands = ['echo Start', fun3d_mpi_command, 'echo Done']

    # submit and wait for job to finish before continuing script
    k4.launch('test_job', pbs_commands)

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _pbs_section:

PBS Job Launcher
%%%%%%%%%%%%%%%%

The PBS class is a tool to define properties of the PBS set up you want to use,
write pbs scripts, and launch jobs.
The ``PBS`` class has several classmethods that serve as alternate constructors which fill in properties of some NASA HPC systems and queue.
Examples of instantiating with this methods is shown below.
For systems or queues not covered by these classmethods, the basic queue attributes are
set in the standard constructor, and less common ones can be adjusted by
changing the attributes of the object.


PBS Class
=========

.. automodule:: pbs4py.pbs

.. autoclass:: PBS
   :members:
   :inherited-members:

PBS's classmethod constructors
------------------------------


.. code-block :: python

    from pbs4py import PBS

    k4 = PBS.k4(time=48)
    k3 = PBS.k3()
    k3a = PBS.k3a()
    nas = PBS.nas(group_list='n1337', proc_type='skylake', time = 72)

FakePBS Class
=============
Some scripts may be set up with the PBS job handler originally, but you may want to run
the script within an existing PBS job without launching new PBS jobs.
The FakePBS object appears to driving scripts as a standard PBS object,
but directly runs the commands instead of putting them into a PBS job and launching the job.

.. autoclass:: FakePBS
   :members:

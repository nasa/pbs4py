from pbs4py import PBS

group = 'a1234'  # replace your charge number here
nas = PBS.nas(group, proc_type='bro', queue_name='devel', time=2)
commands = [nas.create_mpi_command('nodet_mpi', 'debug')]
nas.requested_number_of_nodes = 4
nas.write_job_file('devel.pbs', 'debug', commands)

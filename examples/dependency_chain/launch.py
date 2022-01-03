from pbs4py import PBS

k3 = PBS.k3(time=1)
k3.mem = '4gb'
k3.requested_number_of_nodes = 1

# test_job2 will wait until test_job1 is done before running
pbs_commands = ['echo Start', 'sleep 1m', 'echo Done']
pbs1_id = k3.launch('test_job1', pbs_commands, blocking=False)

new_commands = ['echo Start 2', 'sleep 2m', 'echo Done 2']
k3.launch('test_job2', new_commands, blocking=False, dependency=pbs1_id)

import tailFirst

for jobs_num in range(1,15):
	for worker_num in range(1,10):
		for tasks_num in range(1,5):
			for i in range(1,2000):
				flag = tailFirst.main(jobs_num, worker_num, tasks_num, probRatio=2)
				if flag:
					print(jobs_num,worker_num,tasks_num)
					quit()

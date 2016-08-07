import sys,os,argparse
import random
import heapq
import numpy as np
from numpy import mean
from copy import deepcopy
from itertools import groupby
from operator import itemgetter
from math import floor

def executionTime():
	sample = np.random.pareto(1,1)+1
	return round(sample[0],1)

def jobSizeGenerate():
	return int(random.expovariate(0.025))

def mockPlace(heap, tasks_num,duration):
	candidates=[]
	for k in range(tasks_num):
		(time,index) = heapq.heappop(heap)
		candidates.append((duration,index))
		heapq.heappush(heap, (time+duration,index))
	return candidates

def main(jobs_num, worker_num, probRatio=5):
	stfSet=[]
	srjfSet=[]
	fifoSet=[]
	taSet=[]
	dsrjfSet=[]
	speedupOverSTF=[]
	speedupOverFIFO=[]
	speedupOverta =[]
	speedupOverDSRJF = []
	for iteration in range(20):
		workers = [[] for i in range(worker_num)]
		schedulers = []
		for jobIndex in range(jobs_num):
			tasks_num = max(jobSizeGenerate(),1)
			schedulers.append((jobIndex,tasks_num,executionTime()))
		while(len(schedulers)>0):
			next = random.randint(0,len(schedulers)-1)
			probs = random.sample(range(worker_num), min(worker_num, probRatio))
			#convert to list of waiting time of each worker
			minworker = min(probs, key = lambda x: sum([a for (a,b) in workers[x]]))			
			(ji, tn, duration) = schedulers[next]
			assert(tn >0)
			workers[minworker].append((duration,ji))
			if(tn <=1 ):
				schedulers.pop(next) 
			else:
				schedulers[next] = (ji,tn-1,duration)

		stf = STF(deepcopy(workers))
		srjf = SRJF(deepcopy(workers))
		fifo = FIFO(deepcopy(workers),2)
		(ta) = tailAware(deepcopy(workers))
		dsrjf = DistributedSRJF(deepcopy(workers))
		if(srjf >= dsrjf):
			print'*'*30
		stfSet.append(stf)
		srjfSet.append(srjf)
		fifoSet.append(fifo)
		taSet.append(ta)
		dsrjfSet.append(dsrjf)
		speedupOverta.append(float(ta)/srjf)
		speedupOverFIFO.append(float(fifo)/srjf)
		speedupOverSTF.append(float(stf)/srjf)
		speedupOverDSRJF.append(float(dsrjf)/srjf)
		print("stf",stf,"srjf",srjf,"dsrjf",dsrjf,"fifo",fifo,"ta",ta)
	print ("STF:", mean(stfSet))
	print ("SRJF:", mean(srjfSet))
	print ("FIFO:", mean(fifoSet))
	print ("TailAware", mean(taSet))
	print ("DSRJF:", mean(dsrjfSet))
	#print ("speedupOverSTF", speedupOverSTF)
	#print ("speedupOverFIFO", speedupOverFIFO)
	#print ("len(ta)",len(ta),"len(fifo)",len(fifo),"len(srjf)",len(srjf),"len(workers)",\
	#	len(set([x for worker in workers for (y,x) in worker])))
	print ("speedupOverta==================")
	print ("max",max(speedupOverta),"min",min(speedupOverta),"mean",mean(speedupOverta))
	print ("speedupOverFIFO==================")
	print ("max",max(speedupOverFIFO),"min",min(speedupOverFIFO),"mean",mean(speedupOverFIFO))
	print ("speedupOverSTF==================")
	print ("max",max(speedupOverSTF),"min",min(speedupOverSTF),"mean",mean(speedupOverSTF))	
	print ("speedupOverDSRJF==================")
	print ("max",max(speedupOverDSRJF),"min",min(speedupOverDSRJF),"mean",mean(speedupOverDSRJF))	
	# print "srjf================"	
	# print srjf
	# print "fifo================"
	# print fifo
	# print "ta=================="
	# print ta

def DistributedSRJF(placements):
	def tallyEachJob(placements):
		items = [item for sublist in placements for item in sublist]
		items.sort(key=itemgetter(1))
		remainWork = [reduce(lambda x,y: (x[0]+y[0],x[1]),group) \
			for _,group in groupby(items, key=itemgetter(1))]
		remainWork.sort(key=itemgetter(0))
		return dict([(y,x) for (x,y) in remainWork])

	remWork = tallyEachJob(placements)
	for li in placements:
		li.sort(key=lambda (duration, index):remWork[index])
	return FIFO(deepcopy(placements),1)




def SRJF(placements):
	#placements is a list of list of tasks(duration, jobIndex) on a worker
	execLog = []
	timeAccu =[0]*len(placements)
	jobOrder =[]
	totalRemainTasks = sum([len(x) for x in placements])
	JobTotal= {}
	updatesOfRemWork=[]

	def tallyEachJob(placements):
		items = [item for sublist in placements for item in sublist]
		items.sort(key=itemgetter(1))
		remainWork = [reduce(lambda x,y: (x[0]+y[0],x[1]),group) \
			for _,group in groupby(items, key=itemgetter(1))]
		remainWork.sort(key=itemgetter(0))
		return dict([(y,x) for (x,y) in remainWork])

	def findClosestKey(JobTracker, key):
		assert(key >=0)
		keys = JobTracker.keys()
		keys.sort(reverse=True)
		for k in keys:
			if k <= key:
				return k

	def readRemWork(time):
		effectiveUpdates = [(y,z) for (x,y,z) in updatesOfRemWork if x<=time]
		JTCopy = deepcopy(JobTotal)
		for (duration,jobIndex) in effectiveUpdates:
			JTCopy[jobIndex] -= duration
		return JTCopy



	JobTotal= tallyEachJob(placements)
	heap =[]
	for i in range(len(placements)):
		heap.append((0,i))
	heapq.heapify(heap)
	while(len(heap)>0):
		(curTime, workerID) = heapq.heappop(heap)
		if(len(placements[workerID]) > 0):
			jobOrder = readRemWork(curTime)
			(duration, jobIndex) = min(placements[workerID], key=lambda x: jobOrder[x[1]] )
			placements[workerID].remove((duration,jobIndex))
			updatesOfRemWork.append((curTime+duration,duration,jobIndex))
			execLog.append((curTime+duration,jobIndex))
			heapq.heappush(heap, (curTime+duration,workerID))

	# while(totalRemainTasks > 0):
	# 	for i in range(len(placements)):
	# 		if totalRemainTasks > 0:
	# 			mostRecentKey = findClosestKey(JobTracker,timeAccu[i])
	# 			jobOrder = JobTracker[mostRecentKey]
	# 			if(len(placements[i]) > 0):
	# 				(duration, jobIndex) = min(placements[i], key=lambda x: jobOrder[x[1]] )
	# 				placements[i].remove((duration,jobIndex))
	# 				timeAccu[i] += duration
	# 				JobTracker[timeAccu[i]]=tallyEachJob(placements)
	# 				execLog.append((timeAccu[i],jobIndex))
	# 				totalRemainTasks -= 1

	execLog.sort(key=itemgetter(1))
	JCT = [reduce(lambda x,y: (max(x[0],y[0]),x[1]), group) for _,group in groupby(execLog,key=itemgetter(1))]
	return sum([x for (x,y) in JCT])


def STF(placements):
	execLog=[]
	for li in placements:
		li.sort(key=itemgetter(0))

	for li in placements:
		acc = 0
		#print li
		for (duration, jobIndex) in li:
			execLog.append( (duration+acc, jobIndex) )
			acc += duration
	execLog.sort(key=itemgetter(1))
	JCT = [reduce(lambda x,y: (max(x[0],y[0]),x[1]), group) for _,group in groupby(execLog,key=itemgetter(1))]
	return sum([x for (x,y) in JCT])

def FIFO(placements,flag):
	execLog=[]
	for li in placements:
		acc = 0
		# if(flag>=2):
		# 	#print li
		for (duration, jobIndex) in li:
			execLog.append( (duration+acc, jobIndex) )
			acc += duration
	execLog.sort(key=itemgetter(1))
	JCT = [reduce(lambda x,y: (max(x[0],y[0]),x[1]), group) for _,group in groupby(execLog,key=itemgetter(1))]
	if(flag>=1):
		return sum([x for (x,y) in JCT])
	else:
		return dict([(y,x) for (x,y) in JCT])


def tailAware(placements):
	
	def checkAndPerform(worker,budgetsPerWorker,indexOfBottleneck):
		cur = indexOfBottleneck
		if(cur <= 0):
			return 
		myJobIndex = worker[cur][1]
		leftJobIndex = worker[cur-1][1]
		if worker[cur-1][0] > worker[cur][0] \
			and budgetsPerWorker[leftJobIndex] >= worker[cur][0]:
			budgetsPerWorker[myJobIndex] += worker[cur-1][0]
			budgetsPerWorker[leftJobIndex] -= worker[cur][0]
			temp = worker[cur]
			worker[cur] = worker[cur-1]
			worker[cur-1] = temp
			checkAndPerform(worker, budgetsPerWorker, indexOfBottleneck-1)
	#merge tasks of the same job on every worker
	for li in placements:
		if(len(li) > 1):
			k = 1
			while(k < len(li)):
				if(li[k][1] == li[k-1][1]):
					li[k-1] = (li[k-1][0]+li[k][0], li[k-1][1])
					li.pop(k)
				else:
					k += 1

	tailLatency = FIFO(deepcopy(placements),0)
	budgets = [{} for i in range(len(placements))]
	#update budgets for each task on each worker
	for ii in range(len(placements)):
		acc = 0
		for (duration, jobIndex) in placements[ii]:
			budgets[ii][jobIndex] = tailLatency[jobIndex]-(acc+duration)
			assert(budgets[ii][jobIndex]>=0)
			acc += duration
	for ii in range(len(placements)):
		#print placements[ii]
		for jj in range(len(placements[ii])):
			checkAndPerform(placements[ii], budgets[ii], jj)
	#print ("avg exchangeCount per worker", float(exchangeCount)/len(placements))

	return (FIFO(deepcopy(placements),1))
	

if __name__ == "__main__":
	placements = [[(3.3, 0), (1.6, 1), (1.6, 1)],
		[(1.1, 3), (1.1, 3), (3.3, 0), (1.3, 2)],
		[(1.1, 3), (1.3, 2), (1.6, 1), (1.3, 2), (3.3, 0)]]
	parser = argparse.ArgumentParser()
	parser.add_argument("jobNum", type=int, help= "specify how many jobs")
	parser.add_argument("workerNum", type=int, help= "how many workers")
	#parser.add_argument("tasksNum", type=int, help = "how many tasks in a job")
	#parser.add_argument("taskHeterogeneity", type=int, help="ratio of the longest task over the shrotest task")
	#parser.add_argument("probRatio", type=int, help = "how many probs for each task",default=2)
	#parser.add_argument("iterations", type=int, help="specify the number of iterations",default=1)
	args = parser.parse_args()
	main(args.jobNum,args.workerNum)

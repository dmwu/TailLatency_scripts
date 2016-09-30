import sys,os,argparse
import numpy as np
import Queue
import math
import random
import logging
import Config as cfg
from Config import Task
from abc import ABCMeta, abstractmethod
from numpy import mean
from copy import deepcopy
from itertools import groupby
from operator import itemgetter
from math import floor

def taskGeneration(taskid,currentTime):
	duration = float((np.random.pareto(cfg.paretoA)+1)*cfg.paretoK)
	duration = min(duration,cfg.maxTaskDuration)
	memDemand = (duration-cfg.paretoK)/(cfg.maxTaskDuration-cfg.paretoK)*\
	(cfg.memUpper-cfg.memLower) + cfg.memLower
	return Task(taskid, duration,memDemand, currentTime)

class Event(object):
	""" Abstract class representing events. """
	__metaclass__ = ABCMeta

	@abstractmethod
	def __init__(self):
		pass

	@abstractmethod
	def run(self, current_time):
		""" Returns any events that should be added to the queue. """
		pass

class TaskArrivalWithTrace(Event):
	def __init__(self, worker, taskid, taskTrace):
		self.worker = worker
		self.taskid = taskid
		self.taskTrace = taskTrace
	def run(self, currentTime):
		task = self.taskTrace[self.taskid]
		self.worker.centralQueue.append(task)
		self.worker.maxQueueLength = max(self.worker.maxQueueLength,len(self.worker.centralQueue))
		taskAssignEvent = (currentTime, TaskAssign(self.worker))
		logging.debug('task arrival (id, duration, memDemand) = (%d,%f,%f) at time %f\n',\
				task.taskid, task.duration,task.memDemand, currentTime)
		if (self.taskid < len(self.taskTrace)-1):
			nextArrivalTime = self.taskTrace[self.taskid+1].arrivalTime
			assert(nextArrivalTime>=currentTime)
			taskArrivalWithTraceEvent = (nextArrivalTime,\
				TaskArrivalWithTrace(self.worker,self.taskid+1, self.taskTrace))
			return [taskArrivalWithTraceEvent, taskAssignEvent]
		else:
			return [taskAssignEvent]

class TaskArrival(Event):
	def __init__(self, worker, interArrivalDelay,taskid,taskTrace):
		self.worker = worker
		self.interArrivalDelay = interArrivalDelay
		self.taskid = taskid
		self.taskTrace = taskTrace
	def run(self, currentTime):
		task = taskGeneration(self.taskid,currentTime)
		res = []
		if self.taskid < cfg.numTasks:
			self.taskTrace.append(task)
			self.worker.centralQueue.append(task)
			self.worker.maxQueueLength = max(self.worker.maxQueueLength,len(self.worker.centralQueue))
			taskAssignEvent = (currentTime, TaskAssign(self.worker))
			res.append(taskAssignEvent)
			logging.debug('task arrival (id, duration, memDemand) = (%d,%f,%f) at time %f\n',\
				task.taskid, task.duration,task.memDemand, currentTime)
			arrivalDelay = random.expovariate(1.0 / self.interArrivalDelay)
			#arrivalDelay = self.interArrivalDelay
			if (self.taskid < cfg.numTasks -1):
				taskArrivalEvent = (currentTime + arrivalDelay, TaskArrival(self.worker,\
			 	self.interArrivalDelay, self.taskid+1, self.taskTrace))
				res.append(taskArrivalEvent)
		return res

class TaskAssign(Event):
	# if there's enough mem, put one task into the first execution queue
	def __init__(self,worker):
		self.worker = worker
	def run(self, currentTime):
		res = []
		if(len(self.worker.centralQueue)>0):
			task = self.worker.centralQueue[0]
			if(self.worker.usedMem + task.memDemand <= self.worker.memCapacity):
				self.worker.usedMem += task.memDemand
				self.worker.centralQueue.pop(0)
				assert(task.mode == 'notStarted')
				task.mode = 'waiting'
				if (len(self.worker.emptyCores[0])> 0):
					self.worker.queues[0].append(task)
					#default action is to put the task on the first queue
					taskProcessingEvent = (currentTime, TaskProcessing(self.worker, 0))
					res.append(taskProcessingEvent)
					logging.debug("assign task %d on the first queue at time %f\n",task.taskid, currentTime)
				else:
					for x in xrange(cfg.numQueues-1,0,-1):
						if(len(self.worker.emptyCores[x])>0):
							self.worker.queues[x].append(task)
							taskProcessingEvent = (currentTime,TaskProcessing(self.worker,x))
							res.append(taskProcessingEvent)
							logging.debug("assign task %d on the queue %d at time %f\n",task.taskid, x, currentTime)
							break
				if(len(res)==0):
					self.worker.queues[0].append(task)
					#all queues are busy, put it on the first queue
					taskProcessingEvent = (currentTime, TaskProcessing(self.worker, 0))
					res.append(taskProcessingEvent)
					logging.debug("assign task %d on the first queue at time %f\n",task.taskid, currentTime)
				assert(len(res)==1)

			else:
				#evit the task that takes the most amount of memory
				for x in xrange(cfg.numQueues-1,0,-1):
					if(len(self.worker.queues[x])>0):
						maxMemTask = max(self.worker.queues[x],key = lambda xx:xx.memDemand)
						self.worker.queues[x].remove(maxMemTask)
						if(maxMemTask.mode =='processing'):
							#temp solution just add a core 0 to the queue
							maxMemTask.info = maxMemTask.info+1
							self.worker.emptyCores[x].append(0)
							taskProcessingEvent = (currentTime,TaskProcessing(self.worker,x))
							res.append(taskProcessingEvent)
						else:
							maxMemTask.info = maxMemTask.info + 10
						maxMemTask.mode = 'notStarted'
						maxMemTask.remTime = maxMemTask.duration
						self.worker.usedMem -= maxMemTask.memDemand
						self.worker.centralQueue.append(maxMemTask)
						taskAssignEvent = (currentTime,TaskAssign(self.worker))
						res.append(taskAssignEvent)
		return res


class TaskProcessing(Event):

	def __init__(self, worker, queueid):
		self.worker = worker
		self.queueid = queueid

	def run(self, currentTime):
		res = []
		if(self.worker.emptyCores[self.queueid]):
			frontTask = None
			queueLen = len(self.worker.queues[self.queueid])
			for ii in range(queueLen-1,-1,-1):
				if(self.worker.queues[self.queueid][ii].mode == 'waiting'):
					frontTask = self.worker.queues[self.queueid][ii]
					break
			if(frontTask is not None):
				coreid = self.worker.emptyCores[self.queueid].pop(0)
				assert(frontTask.mode == 'waiting')
				frontTask.mode = 'processing'
				logging.debug("processing task %d on queue %d on core %d at time %f\n",\
					frontTask.taskid,self.queueid,coreid, currentTime)
				if(frontTask.remTime <= self.worker.thresholds[self.queueid]):	
					taskEndEvent = (currentTime+frontTask.remTime, TaskEnd(self.worker, frontTask, self.queueid,coreid))
					res.append(taskEndEvent)
				else:
					assert(self.queueid < cfg.numQueues-1)
					curThreshold = self.worker.thresholds[self.queueid]
					taskMigrationEvent = (currentTime+cfg.switchingOverhead+curThreshold, \
						TaskMigration(self.worker, frontTask, self.queueid, coreid))
					res.append(taskMigrationEvent)
			elif (frontTask is None and len(self.worker.centralQueue) > 0):
				logging.debug("the first queue is empty, \
					a new taskAssignEvent is scheduled at time %f\n",currentTime)
				taskAssignEvent = (currentTime, TaskAssign(self.worker))
				res.append(taskAssignEvent)
			assert(len(res)<=1)
		return res



class TaskMigration(Event):
	def __init__(self, worker, task, curQueueid,curCoreid):
		self.worker = worker
		self.task = task
		self.curQueueid = curQueueid
		self.desQueueid = curQueueid+1
		self.curCoreid = curCoreid
	def run(self,currentTime):
		res = []
		if(self.task in self.worker.queues[self.curQueueid]):
			self.worker.queues[self.curQueueid].remove(self.task)
			self.worker.emptyCores[self.curQueueid].append(self.curCoreid)
			assert(self.task.mode == 'processing')
			self.task.mode = 'waiting'
			self.task.remTime -= self.worker.thresholds[self.curQueueid]
			self.worker.queues[self.desQueueid].append(self.task)
			self.worker.busyTime[self.curCoreid] += self.worker.thresholds[self.curQueueid]
			logging.debug("migrating task %d from queue %d to queue %d at time %f\n",\
				self.task.taskid, self.desQueueid-1,self.desQueueid,currentTime)
			taskProcessingEvent1 = (currentTime, TaskProcessing(self.worker, self.curQueueid))
			res.append(taskProcessingEvent1)
			if(len(self.worker.emptyCores[self.desQueueid]) > 0):
				#just moved to an empty queue
				logging.debug("the destination queue is empty\n")
				taskProcessingEvent2 = (currentTime, TaskProcessing(self.worker, self.desQueueid))
				res.append(taskProcessingEvent2)
		return res


class TaskEnd(Event):
	def __init__(self, worker, task, queueid,coreid):
		self.worker = worker
		self.task = task
		self.queueid = queueid
		self.coreid = coreid
	def run(self, currentTime):
		res = []
		if(self.task in self.worker.queues[self.queueid]):
			self.worker.endTaskCounter += 1
			logging.debug("task %d end on queue %d core %d at time %f\n",\
			self.task.taskid,self.queueid,self.coreid,currentTime)
			self.worker.queues[self.queueid].remove(self.task)
			self.worker.busyTime[self.coreid] += self.task.remTime
			self.worker.emptyCores[self.queueid].append(self.coreid)
			if(not self.task.mode == 'processing'):
				print self.task.mode
				print self.task.info
				print self.task.memDemand,self.task.duration
				sys.exit(0)
			self.task.mode = 'done'
			self.task.remTime =0
			self.worker.usedMem -= self.task.memDemand
			slowdown = (currentTime - self.task.arrivalTime)/self.task.duration
			logging.info("task %d arrivals at %f with duration %f, finishs at %f, slowdown is %f\n",\
				self.task.taskid, self.task.arrivalTime, self.task.duration, currentTime, slowdown)
			self.worker.slowDownStat[self.task.taskid] = slowdown
			self.worker.flowTimeStat[self.task.taskid] = currentTime - self.task.arrivalTime
			if (len(self.worker.queues[self.queueid]) >0 ):
				taskProcessingEvent = (currentTime, TaskProcessing(self.worker,self.queueid))
				res.append(taskProcessingEvent)
			#print "in task end event post a processing event on queue %d,at time %f queue len%d"%(self.queueid,currentTime,len(self.worker.queues[self.queueid]))
			taskAssignEvent = (currentTime,TaskAssign(self.worker))
			res.append(taskAssignEvent)
			if (self.worker.endTaskCounter == self.worker.numTasks):
				self.worker.terminationTime = currentTime
		return res

class Worker(object):
	def __init__(self, id,load, numTasks=0):
		self.memCapacity = cfg.memoryCapacity
		self.usedMem = 0
		self.cores_per_queue = cfg.totalNumOfCores/cfg.numQueues
		assert(type(self.cores_per_queue) is int)
		self.centralQueue = []
		self.queues = [[] for i in xrange(cfg.numQueues)]
		self.id = id
		self.endTaskCounter = 0
		self.maxQueueLength = 0
		self.thresholds =cfg.loadAndThresholds[load][:]
		self.thresholds.append(float('inf'))
		assert(cfg.numQueues == len(self.thresholds))
		logging.warning("thresholds:%s\n",str([x for x in self.thresholds]))
		self.slowDownStat = {}
		self.flowTimeStat = {}
		self.busyTime = [0 for x in xrange(cfg.totalNumOfCores)]
		self.terminationTime = 0
		self.emptyCores = {}
		self.numTasks = numTasks
		for x in xrange(cfg.numQueues):
			self.emptyCores[x] = [x*self.cores_per_queue+y for y in xrange(self.cores_per_queue)]

def get_percentile(N, percent):
    if not N:
        return -1
    k = (len(N) - 1) * percent/100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return N[int(k)]
    d0 = N[int(f)] * (c-k)
    d1 = N[int(c)] * (k-f)
    return d0 + d1

def boundedParetoMedian(l,h,a):
	assert(l<=h)
	temp = 1-0.5*(1-(l/h)**a)
	res = l*temp**(-1/a)
	return res
	
def boundedParetoMeanWiki(l,h,a):
	assert(not a==1)
	temp = l**a/(1-(l/h)**a)
	res = temp*a/(a-1)*(1/l**(a-1)-1/h**(a-1))
	return res


class EvictMigration(object):
	def __init__(self,flag,load, trace=[]):
		self.centralQueue = []
		self.mean = boundedParetoMeanWiki(cfg.paretoK,cfg.maxTaskDuration,cfg.paretoA)
		self.median = boundedParetoMedian(cfg.paretoK,cfg.maxTaskDuration,cfg.paretoA)
		self.interArrivalDelay = self.mean/(load*cfg.totalNumOfCores)
		self.eventQueue = Queue.PriorityQueue()
		
		self.fromTrace = flag
		if(self.fromTrace):
			assert(len(trace)>0)
			self.taskTrace = trace
			self.worker = Worker(0,load,len(trace))
		else:
			self.taskTrace = []
			self.worker = Worker(0,load)

	def run(self):
		if (self.fromTrace):
			assert(len(self.taskTrace)>0)
			self.eventQueue.put((0,TaskArrivalWithTrace(self.worker,0,self.taskTrace)))
		else:
			self.eventQueue.put((0,TaskArrival(self.worker,self.interArrivalDelay,0,self.taskTrace)))
			#logging.critical("dist mean:%f, dist median:%f,interArrivalDelay:%f\n",\
			#	self.mean,self.median,self.interArrivalDelay)
		lastTime = 0
		while not self.eventQueue.empty():
			(currentTime, event) = self.eventQueue.get()
			assert currentTime >= lastTime
			lastTime = currentTime
			#print (type(event))
			newEvents = event.run(currentTime)
			for newEvent in newEvents:
				self.eventQueue.put(newEvent)
		if(not self.fromTrace):
			assert(self.worker.endTaskCounter == cfg.numTasks)
		else:
			assert(self.worker.endTaskCounter == len(self.taskTrace))
		slowdowns = self.worker.slowDownStat.values()
		slowdowns.sort()
		ft = self.worker.flowTimeStat.values()
		median = get_percentile(slowdowns,50)
		percentile99 = get_percentile(slowdowns,99)
		meanSlowdown = np.mean(slowdowns)
		meanFt = np.mean(ft)
		logging.warning("Migration average slowdown is %f\n", meanSlowdown)
		logging.warning("Migration max slowdown is %f\n",max(slowdowns))
		logging.warning("Migration average flowTime is %f\n",meanFt)
		#logging.critical("max queue length achieved:%d\n",self.worker.maxQueueLength)
		print "evict migration sum of workload:%f terminationTime:%f"%\
		(sum(self.worker.busyTime),self.worker.terminationTime)
	#	for x in self.worker.busyTime:
	#		print "evict migration cpu utilization:%f"%(x/self.worker.terminationTime)
		return (self.taskTrace, meanSlowdown, meanFt)

def main():
	logging.basicConfig(level=logging.DEBUG, format='%(message)s')
	customTrace=[cfg.Task(0, 20.0, 3.0, 0), cfg.Task(1, 21.0, 3.0, 0.2),\
		   cfg.Task(2, 0.2, 3.0, 0.3), cfg.Task(3, 0.21, 3.0, 0.4)]
	migration = BoostMigration(True, customTrace)
	trace = migration.run()
if __name__ == "__main__":
	main()

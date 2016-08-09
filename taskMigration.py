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
	duration = float((np.random.pareto(cfg.paretoA,1)+1)*cfg.paretoK)
	duration = round(min(duration,cfg.maxTaskDuration),3)
	memDemand = min(1,(duration*10000/cfg.maxTaskDuration))*\
	(cfg.memUpper-cfg.memLower) + cfg.memLower
	memDemand = round(memDemand,3)
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
		taskAssignEvent = (currentTime, TaskAssign(self.worker))
		logging.debug('task arrival (id, duration, memDemand) = (%d,%f,%f) at time %f\n',\
				task.taskid, task.duration,task.memDemand, currentTime)
		if (self.taskid < len(self.taskTrace)-1):
			arrivalDelay = self.taskTrace[self.taskid+1].arrivalTime
			taskArrivalEvent = (currentTime + arrivalDelay,\
			 TaskArrivalWithTrace(self.worker,self.taskid+1, self.taskTrace))
			return [taskArrivalEvent, taskAssignEvent]
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
		if self.taskid < cfg.numTasks:
			self.taskTrace.append(task)
			self.worker.centralQueue.append(task)
			taskAssignEvent = (currentTime, TaskAssign(self.worker))
			logging.debug('task arrival (id, duration, memDemand) = (%d,%f,%f) at time %f\n',\
				task.taskid, task.duration,task.memDemand, currentTime)
			arrivalDelay = round(random.expovariate(1.0 / self.interArrivalDelay),3)
			taskArrivalEvent = \
			(currentTime + arrivalDelay, TaskArrival(self.worker,\
			 self.interArrivalDelay, self.taskid+1, self.taskTrace))
			return [taskArrivalEvent, taskAssignEvent]
		else:
			return []

class TaskAssign(Event):
	# if there's enough mem, put one task into the first execution queue
	def __init__(self,worker):
		self.worker = worker
	def run(self, currentTime):
		if(len(self.worker.centralQueue)>0):
			task = self.worker.centralQueue[0]
			if( self.worker.usedMem + task.memDemand <= self.worker.memCapacity):
				self.worker.usedMem += task.memDemand
				self.worker.centralQueue.pop(0)
				self.worker.queues[0].append(task)
				logging.debug("assign task %d on the first queue at time %f\n",task.taskid, currentTime)
				if (len(self.worker.queues[0])==1):
					#just moved to the first queue which is empty
					taskProcessingEvent = (currentTime, TaskProcessing(self.worker, 0))
					return [taskProcessingEvent]
		return []


class TaskProcessing(Event):

	def __init__(self, worker, queueid):
		self.worker = worker
		self.queueid = queueid

	def run(self, currentTime):
		if(len(self.worker.queues[self.queueid])>0):
			frontTask = self.worker.queues[self.queueid][0]
			logging.debug("processing task %d on queue %d at time %f\n",\
				frontTask.taskid,self.queueid,currentTime)
			if(frontTask.remTime <= self.worker.thresholds[self.queueid]):	
				taskEndEvent = (currentTime+frontTask.remTime, TaskEnd(self.worker, frontTask, self.queueid))
				return [taskEndEvent]
			else:
				assert(self.queueid < cfg.numCores-1)
				curThreshold = self.worker.thresholds[self.queueid]
				taskMigrationEvent = (currentTime+cfg.switchingOverhead+curThreshold, \
					TaskMigration(self.worker, frontTask, self.queueid, self.queueid+1))
				return [taskMigrationEvent]
		elif (self.queueid == 0 and len(self.worker.centralQueue) > 0):
			logging.debug("the first queue is empty, \
				a new taskAssignEvent is scheduled at time %f\n",currentTime)
			taskAssignEvent = (currentTime, TaskAssign(self.worker))
			return [taskAssignEvent]

		else:
			return []



class TaskMigration(Event):
	def __init__(self, worker, task, curID, desID):
		self.worker = worker
		self.task = task
		self.curID = curID
		self.desID = desID
		assert(desID >=1 )
	def run(self,currentTime):
		self.worker.queues[self.curID].pop(0)
		self.task.remTime -= self.worker.thresholds[self.curID]
		self.worker.queues[self.desID].append(self.task)
		logging.debug("migrating task %d from queue %d to queue %d at time %f\n",\
			self.task.taskid, self.desID-1,self.desID,currentTime)
		taskProcessingEvent1 = (currentTime, TaskProcessing(self.worker, self.curID))
		if(len(self.worker.queues[self.desID]) == 1):
			#just moved to an empty queue
			logging.debug("the destination queue is empty\n")
			taskProcessingEvent2 = (currentTime, TaskProcessing(self.worker, self.desID))
			return [taskProcessingEvent1, taskProcessingEvent2]
		else:
			return [taskProcessingEvent1]


class TaskEnd(Event):
	def __init__(self, worker, task, queueid):
		self.worker = worker
		self.task = task
		self.queueid = queueid
	def run(self, currentTime):
		logging.debug("task %d end on queue %d at time %f\n",self.task.taskid,self.queueid,currentTime)
		self.worker.queues[self.queueid].pop(0)
		self.task.remTime =0
		self.worker.usedMem -= self.task.memDemand
		self.worker.finishTime[self.task.taskid] = currentTime
		slowdown = (currentTime - self.task.arrivalTime)/self.task.duration
		logging.info("task %d arrivals at %f with duration %f, finishs at %f, slowdown is %f\n",\
			self.task.taskid, self.task.arrivalTime, self.task.duration, currentTime, slowdown)
		self.worker.slowDownStat[self.task.taskid] = slowdown
		taskProcessingEvent = (currentTime, TaskProcessing(self.worker,self.queueid))
		return [taskProcessingEvent]

class Worker(object):
	def __init__(self, id):
		self.memCapacity = cfg.memoryCapacity
		self.usedMem = 0
		self.centralQueue = []
		self.queues = []
		self.id = id
		self.thresholds =[3**k*0.1 for k in range(cfg.numCores-1)]
		logging.critical("thresholds:%s\n",str([x for x in self.thresholds]))
		self.slowDownStat = {}
		self.thresholds.append(np.inf)
		self.finishTime = {}
		self.arrivalTime = {}
		while len(self.queues) < cfg.numCores:
			self.queues.append([])

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


class Migration(object):
	def __init__(self,flag,trace=[]):
		self.centralQueue = []
		mean = boundedParetoMedian(cfg.paretoK,cfg.maxTaskDuration,cfg.paretoA)
		self.interArrivalDelay = mean/(cfg.load*cfg.numCores)
		logging.warning("pareto median:%f interArrivalDelay: %f\n",mean,self.interArrivalDelay)
		self.eventQueue = Queue.PriorityQueue()
		self.worker = Worker(0)
		self.fromTrace = flag
		if(self.fromTrace):
			assert(len(trace)>0)
			self.taskTrace = trace
		else:
			self.taskTrace = []

	def run(self):
		if (self.fromTrace):
			assert(len(self.taskTrace)>0)
			self.eventQueue.put((0,TaskArrivalWithTrace(self.worker,0,self.taskTrace)))
		else:
			self.eventQueue.put((0,TaskArrival(self.worker,self.interArrivalDelay,0,self.taskTrace)))
		lastTime = 0
		while not self.eventQueue.empty():
			(currentTime, event) = self.eventQueue.get()
			assert currentTime >= lastTime
			lastTime = currentTime
			#print (type(event))
			newEvents = event.run(currentTime)
			for newEvent in newEvents:
				self.eventQueue.put(newEvent)
		slowdowns = self.worker.slowDownStat.values()
		slowdowns.sort()
		median = get_percentile(slowdowns,50)
		percentile99 = get_percentile(slowdowns,99)
		mean = np.mean(slowdowns)
		logging.warning("Migration average slowdown is %f\n", mean)
		logging.warning("Migration max slowdown is %f\n",max(slowdowns))
		logging.warning("Migration median and 99th percentile slowdowns are %f,%f\n",\
			median,percentile99)
		return (self.taskTrace, mean)

def main():
	logging.basicConfig(level=logging.ERROR, format='%(message)s')
	migration = Migration()
	trace = migration.run()
if __name__ == "__main__":
	main()

import sys,os,argparse
import numpy as np
import Queue
import math
import random
import logging
from abc import ABCMeta, abstractmethod
from numpy import mean
from copy import deepcopy
from itertools import groupby
from operator import itemgetter
from math import floor
#taskDurations = random.pareto(1,1)+1
numCores = 8
memoryCapacity = 128.0
paretoK = 0.05
paretoA = 1.001 #should be larger than 1
maxTaskDuration = 360000.0
memLower = 1 #unit GB
memUpper = 20
taskAssignWaitingTime = 3 # unit second
switchingOverhead = 0.001
numTasks = 30000

class Task(object):
	def __init__(self, duration, memDemand, taskid):
		self.duration = duration
		self.remTime = duration
		self.memDemand = memDemand
		self.taskid = taskid

def taskGeneration(taskid):
	duration = float((np.random.pareto(paretoA,1)+1)*paretoK)
	duration = min(duration,maxTaskDuration)
	memDemand = (duration/maxTaskDuration)*(memUpper-memLower) + memLower
	return Task(duration,memDemand,taskid)

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

class TaskArrival(Event):
	def __init__(self, worker, interArrivalDelay,taskid):
		self.worker = worker
		self.interArrivalDelay = interArrivalDelay
		self.taskid = taskid
	def run(self, currentTime):
		task = taskGeneration(self.taskid)
		self.worker.centralQueue.append(task)
		self.worker.arrivalTime[task.taskid] = currentTime
		taskAssignEvent = (currentTime, TaskAssign(self.worker))
		if self.taskid < numTasks:
			logging.debug('task arrival (duration, memDemand, id) = (%f,%d,%d) at time %f\n',\
				task.duration,task.memDemand,task.taskid, currentTime)
			arrivalDelay = random.expovariate(1.0 / self.interArrivalDelay)
			logging.debug("next task will arrival on time %f\n",currentTime+arrivalDelay)
			taskArrivalEvent = \
			(currentTime + arrivalDelay, TaskArrival(self.worker, self.interArrivalDelay, self.taskid+1))
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
			logging.debug("processing task %d on queue %d at time %f\n",frontTask.taskid,self.queueid,currentTime)
			if(frontTask.remTime <= self.worker.thresholds[self.queueid]):	
				taskEndEvent = (currentTime+frontTask.remTime, TaskEnd(self.worker, frontTask, self.queueid))
				return [taskEndEvent]
			else:
				assert(self.queueid < numCores-1)
				curThreshold = self.worker.thresholds[self.queueid]
				taskMigrationEvent = (currentTime+switchingOverhead+curThreshold, \
					TaskMigration(self.worker, frontTask, self.queueid, self.queueid+1))
				return [taskMigrationEvent]
		elif (self.queueid == 0 and len(self.worker.centralQueue) >0):
			logging.debug("the first queue is empty, a new taskAssignEvent is scheduled at time %f\n",currentTime)
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
		arrivalTime = self.worker.arrivalTime[self.task.taskid]
		slowdown = (currentTime - arrivalTime)/self.task.duration
		logging.info("task %d arrivals at %f with duration %f, finishs at %f, slowdown is %f\n",\
			self.task.taskid, arrivalTime, self.task.duration, currentTime, slowdown)
		self.worker.slowDownStat[self.task.taskid] = slowdown
		taskProcessingEvent = (currentTime, TaskProcessing(self.worker,self.queueid))
		return [taskProcessingEvent]

class Worker(object):
	def __init__(self, id):
		self.memCapacity = memoryCapacity
		self.usedMem = 0
		self.centralQueue = []
		self.queues = []
		self.id = id
		self.thresholds =[2**k*0.1 for k in range(numCores-1)]
		self.slowDownStat = {}
		self.thresholds.append(np.inf)
		self.finishTime = {}
		self.arrivalTime = {}
		while len(self.queues) < numCores:
			self.queues.append([])

class Simulation(object):
	def __init__(self, load):
		self.load = load
		self.centralQueue = []
		self.interArrivalDelay = (paretoA*paretoK/(paretoA-1))/(load*numCores)
		self.eventQueue = Queue.PriorityQueue()
		self.worker = Worker(0)
	def run(self):
		self.eventQueue.put((0,TaskArrival(self.worker,self.interArrivalDelay,0)))
		lastTime = 0
		while not self.eventQueue.empty():
			(currentTime, event) = self.eventQueue.get()
			assert currentTime >= lastTime
			lastTime = currentTime
			#print (type(event))
			newEvents = event.run(currentTime)
			for newEvent in newEvents:
				self.eventQueue.put(newEvent)
		logging.critical("=========average slowdown is=============%f\n",\
			np.mean(self.worker.slowDownStat.values()))


def main():
	logging.basicConfig(level=logging.CRITICAL, format='%(message)s')
	sim = Simulation(0.7)
	sim.run()

if __name__ == "__main__":
	main()



import sys,os,argparse
import numpy as np
import Queue
import math
import random
import logging
from Config import Task
import Config as cfg
from Config import Task
from abc import ABCMeta, abstractmethod
from numpy import mean
from copy import deepcopy
from itertools import groupby
from operator import itemgetter
from math import floor

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
	def __init__(self, worker, trace, taskid):
		self.worker = worker
		self.trace = trace
		self.taskid = taskid
		assert(taskid == trace[taskid].taskid)
	def run(self, currentTime):
		res = []
		task = self.trace[self.taskid]
		currentTime = task.arrivalTime
		logging.debug('task arrival (id, duration, memDemand) = (%d,%f,%f) at time %f\n',\
				task.taskid, task.duration,task.memDemand, currentTime)
		self.worker.centralQueue.append(task)
		if(self.taskid < len(self.trace)-1):
			nextArrivalTime = self.trace[self.taskid+1].arrivalTime
			nextArrivalEvent = (nextArrivalTime,TaskArrival(self.worker,self.trace, self.taskid+1)) 
			res.append(nextArrivalEvent)
		taskAssignEvent = (task.arrivalTime,TaskAssign(self.worker))
		res.append(taskAssignEvent)
		return res


class TaskAssign(Event):
	def __init__(self,worker):
		self.worker = worker
	def run(self, currentTime):
		if (len(self.worker.emptyCores) > 0 ):
			if(len(self.worker.centralQueue)> 0):
				task = self.worker.centralQueue[0]
				if(task.memDemand + self.worker.usedMem <= self.worker.memCapacity):
					self.worker.centralQueue.pop(0)
					self.worker.emptyCores.sort()
					coreid = self.worker.emptyCores.pop(0)
					self.worker.usedMem += task.memDemand
					logging.debug("assign task %d on core %d at time %f\n",task.taskid, coreid, currentTime)
					taskEndEvent = (currentTime+task.duration, TaskEnd(self.worker, coreid, task))
					return [taskEndEvent]
		return []

class TaskEnd(Event):
	def __init__(self,worker,coreid,task):
		self.worker = worker
		self.coreid = coreid
		self.task = task
	def run(self, currentTime):
		self.worker.endTaskCounter += 1
		self.worker.busyTime[self.coreid] += self.task.duration
		self.worker.emptyCores.append(self.coreid)
		self.worker.usedMem -= self.task.memDemand
		slowdown = (currentTime - self.task.arrivalTime)/self.task.duration
		logging.info("task %d arrivals at %f with duration %f, finishs at %f, slowdown is %f\n",\
			self.task.taskid, self.task.arrivalTime, self.task.duration, currentTime, slowdown)
		self.worker.slowDownStat[self.task.taskid] = slowdown
		self.worker.flowTimeStat[self.task.taskid] = currentTime -self.task.arrivalTime
		taskAssignEvent = (currentTime,TaskAssign(self.worker))
		if(self.worker.endTaskCounter == len(self.worker.trace)):
			self.worker.terminationTime = currentTime
		return [taskAssignEvent]

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


class Worker(object):
	def __init__(self,id,trace):
		self.id = id
		self.trace = trace
		self.emptyCores = range(cfg.numCores)
		self.memCapacity = cfg.memoryCapacity
		self.usedMem = 0
		self.centralQueue = []
		self.id = id
		self.slowDownStat = {}
		self.flowTimeStat = {}
		self.busyTime = [0 for x in xrange(cfg.numCores)]
		self.terminationTime = 0
		self.endTaskCounter = 0

class FIFO(object):
	def __init__(self,trace):
		self.eventQueue = Queue.PriorityQueue()
		self.worker = Worker(0,trace)
		self.trace = trace
	def run(self):
		self.eventQueue.put((0,TaskArrival(self.worker,self.trace,0)))
		lastTime = 0
		while not self.eventQueue.empty():
			(currentTime, event) = self.eventQueue.get()
			assert currentTime >= lastTime
			lastTime = currentTime
			#print (type(event))
			newEvents = event.run(currentTime)
			for newEvent in newEvents:
				self.eventQueue.put(newEvent)
		print(self.worker.endTaskCounter, len(self.trace), cfg.numTasks)
		assert(self.worker.endTaskCounter == cfg.numTasks)
		slowdowns = self.worker.slowDownStat.values()
		slowdowns.sort()
		median = get_percentile(slowdowns,50)
		percentile99 = get_percentile(slowdowns,99)
		slowdownMean = np.mean(slowdowns)
		ftMean = np.mean(self.worker.flowTimeStat.values())
		logging.warning("FIFO average slowdown is %f\n",slowdownMean)
		logging.warning("FIFO max slowdown is %f\n",max(slowdowns))
		logging.warning("FIFO flowTime mean is %f\n",ftMean)
		logging.warning("FIFO median and 99th percentile slowdowns are %f,%f\n",median,percentile99)
		for x in self.worker.busyTime:
			print "fifo cpu utilization:%f"%(x/self.worker.terminationTime)
		print "fifo sum of workload:%f terminationTime:%f"%\
		(sum(self.worker.busyTime),self.worker.terminationTime)
		return (slowdownMean,ftMean)

def main():
	logging.basicConfig(level=logging.INFO, format='%(message)s')
	task = Task(1,1,0,0)
	trace =[task]
	fifo = FIFO(trace)
	fifo.run()
if __name__ == "__main__":
	main()




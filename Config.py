numCores = 2
memoryCapacity = 128.0
paretoK = 0.01
paretoA = 0.8
maxTaskDuration = 36000.0
memLower = 1 #unit GB
memUpper = 10
switchingOverhead = 0.0
load = 0.85
numTasks = 1000000
networkBandwidth = 1024*1024*1000.0 #1000MB
diskBandwidth = 1024*1024*100.0 #100MB
CPUspeed = 2*1024*1024*1024.0 #2GHz

class Task(object):
	def __init__(self, taskid, duration, memDemand,arrivTime):
		self.duration = duration
		self.remTime = duration
		self.memDemand = memDemand
		self.taskid = taskid
		self.arrivalTime = arrivTime
	def __str__(self):
		return "id:%d, duration:%f, memDemand:%f, arrivalTime:%f\n"\
		%(self.taskid,self.duration,self.memDemand,self.arrivalTime)
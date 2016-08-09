numCores = 4
memoryCapacity = 128.0
paretoK = 0.01
paretoA = 0.8 
maxTaskDuration = 360000.0
memLower = 1 #unit GB
memUpper = 20
switchingOverhead = 0.001
load = 0.7
numTasks = 1000000

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
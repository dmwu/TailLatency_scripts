numQueues = 2
loadAndThresholds = {0.3:[830],0.4:[840],0.5:[859],0.6:[891],\
0.7:[945],0.71:[952],0.72:[960],0.73:[967],0.74:[976],\
0.75:[985],0.76:[994],0.77:[1004],0.78:[1015],0.79:[1026],0.8:[1037],\
0.9:[1197],0.92:[1240],0.94:[1286],0.96:[1335],0.98:[1386]}
memoryCapacity = 128
paretoK = 1.0
paretoA = 0.9
totalNumOfCores = 6
maxTaskDuration = 3.6e5
memLower = 0.001*memoryCapacity#unit GB
memUpper = 0.07*memoryCapacity
switchingOverhead = 0.01
load = 0.4
numTasks = 8e5
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
		self.mode = 'notStarted'
	def __str__(self):
		return "id:%d, duration:%f, memDemand:%f, arrivalTime:%f\n"\
		%(self.taskid,self.duration,self.memDemand,self.arrivalTime)

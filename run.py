import taskMigration
import FIFO
import boostMigration
import random
import evictLongestTask
import numpy as np
import Config as cfg
from Config import Task
import logging
def getSamples(trace, count):
	for x in xrange(min(count,len(trace))):
		yield "duration:%f,arrivalTime:%f\n"%(trace[x].duration,trace[x].arrivalTime)

def traceCheckandRestore(trace):
	assert(len(trace) >0 )
	lastTime = 0

	#print("==========checking",len(trace))
	for x in xrange(len(trace)):
		#print (trace[x].taskid,trace[x].arrivalTime)
		if(trace[x].taskid!=x):
			logging.critical("ERROR! taskid inconsistency in trace,x:%d,\
			 taskid:%d",x,trace[x].taskid)
		if(trace[x].arrivalTime < lastTime):
			logging.critical("ERROR! arrivalTime inconsistency in trace,\
				lastTime:%f,arrivalTime:%f\n",lastTime,trace[x].arrivalTime)
		lastTime = trace[x].arrivalTime
		trace[x].mode = 'notStarted'
		trace[x].remTime = trace[x].duration

def parsingTrace(filename,trace,lastArrivalTime,lastID):
	count = lastID
	with open(filename) as f:
		firstFlag = True
		firstTime = 0
		for line in f:
			ss = line.strip().split()
			if(not len(ss) >= 6):
				continue
			if(firstFlag):
				firstTime = float(ss[1])
				firstFlag = False
			arrivalTime = float(ss[1])-firstTime + lastArrivalTime
			diskDataSize = float(ss[3])+float(ss[5])
			shuffleDataSize = float(ss[4])
			taskDurationEstimation = (diskDataSize/cfg.diskBandwidth \
				+shuffleDataSize/cfg.networkBandwidth)*100
			if(taskDurationEstimation<=0):
				#print "zero task duraion! from file:%s with jobid:%s\n"%(filename,ss[0])
				continue
			trace.append(cfg.Task(count,taskDurationEstimation,random.randint(1,20),arrivalTime))
			#print trace[-1].__str__()
			count += 1
	return count
def getLoadFromTrace(trace):
	assert(len(trace)>0)
	durationsSum = 0.0
	interArrivTimeSum = 0.0
	for x in xrange(len(trace)-1):
		durationsSum += trace[x].duration
		interArrivTimeSum += trace[x+1].arrivalTime-trace[x].arrivalTime
	return (durationsSum/interArrivTimeSum)/cfg.numCores

def getStatisticFromTrace(trace):
	assert(len(trace)>0)
	du = [x.duration for x in trace ]
	return np.mean(du), np.median(du)

def writeDurationstoFile(filename,trace):
	with open(filename,'w+') as f:
		for x in trace:
			f.write(str(x.duration)+"\n")

def varyWithload():
	logging.basicConfig(level=logging.CRITICAL, format='%(message)s')
	for x in xrange(1,30):
		cfg.load = x*0.1
		migration = taskMigration.Migration(False)
		(trace,migrationMean) = migration.run()
		traceCheck(trace)
		fifo = FIFO.FIFO(trace)
		fifomean = fifo.run()
		logging.critical("paretoA:%f, load:%f, speedup:%f\n",\
		cfg.paretoA, cfg.load, fifomean/migrationMean)

def main():
	logging.basicConfig(level=logging.CRITICAL, format='%(message)s')
	# customTrace=[cfg.Task(0, 20.0, 3.0, 0), cfg.Task(1, 21.0, 3.0, 0.2),\
	# 	   cfg.Task(2, 0.2, 3.0, 0.3), cfg.Task(3, 0.21, 3.0, 0.4)]
	# filenames=["FB-2009_samples_24_times_1hr_0.tsv",
	# "FB-2009_samples_24_times_1hr_1.tsv",
	# "FB-2010_samples_24_times_1hr_0.tsv",
	# "FB-2010_samples_24_times_1hr_withInputPaths_0.tsv"
	# ]
	# productionTrace =[]
	# startingTime =0
	# nextID = 0
	# for x in filenames:
	# 	nextID = parsingTrace(x,productionTrace,startingTime,nextID)
	# 	startingTime = productionTrace[-1].arrivalTime
	# for x in getSamples(productionTrace,100):
	# 	print x
	#writeDurationstoFile("duration.txt",productionTrace)
	items = cfg.loadAndThresholds.items()
	items.sort(key = lambda x:x[0])
	for (load,thresds) in items:
		logging.critical("*********cores:%d,load%f with memory %d***********\n",cfg.totalNumOfCores,load,cfg.memoryCapacity)
		migration = taskMigration.Migration(False,load)
		(trace,miSlowdownMean,miFTmean) = migration.run()
		traceCheckandRestore(trace)
		boostmi = boostMigration.BoostMigration(True,load,trace)
		(newtrace,booSlowdown,booFTmean) = boostmi.run()

		traceCheckandRestore(trace)
		evictmi = evictLongestTask.EvictMigration(True,load,trace)
		(newnewtrace,evictSlowdown,evictFt) = evictmi.run()

		fifo = FIFO.FIFO(trace)
		(fifoSlowdownMean,fifoFTmean) = fifo.run()
		#logging.critical("paretoA:%f, givenLoad:%f,calculatedLoad:%f\n",\
		#	cfg.paretoA, cfg.load,getLoadFromTrace(trace))
		duMean,duMedian = getStatisticFromTrace(trace)
		#logging.critical("calculated mean and median:(%f,%f)\n",duMean,duMedian)
		slowdownSpeedup = fifoSlowdownMean/miSlowdownMean
		FTspeedup = fifoFTmean/miFTmean
		boslowdownSpeedup = fifoSlowdownMean/booSlowdown
		boftspeedup = fifoFTmean/booFTmean
		evictSDSpeedup = fifoSlowdownMean/evictSlowdown
		evictFTSpeedup = fifoFTmean/evictFt
		logging.critical("[slowdown]fifo:%3f,migration:%f,boostMigration:%f,evict:%3f,speedup1:%3f speedup2:%3f speedup3:%3f \n",\
			round(fifoSlowdownMean,2),round(miSlowdownMean,2),booSlowdown, evictSlowdown, slowdownSpeedup, boslowdownSpeedup,evictSDSpeedup)
		logging.critical("[flowtime]fifo:%3f,migration:%f,boostMigration:%f,evict:%3f speedup1:%3f speedup2:%3f speedup3:%3f \n",\
			round(fifoFTmean,2),round(miFTmean,2),booFTmean,evictFt, FTspeedup,boftspeedup,evictFTSpeedup)


if __name__ == "__main__":
	main()

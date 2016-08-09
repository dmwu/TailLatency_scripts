import taskMigration
import FIFO
import Config as cfg
from Config import Task
import logging
def getSamples(trace, count):
	for x in xrange(min(count,len(trace))):
		yield round(trace[x].duration,3)
def traceCheck(trace):
	assert(len(trace) >0 )
	lastTime = 0

	#print("==========checking",len(trace))
	for x in range(len(trace)):
		#print (trace[x].taskid,trace[x].arrivalTime)
		if(trace[x].taskid!=x):
			logging.critical("ERROR! taskid inconsistency in trace,x:%d,\
			 taskid:%d",x,trace[x].taskid)
		if(trace[x].arrivalTime < lastTime):
			logging.critical("ERROR! arrivalTime inconsistency in trace,\
				lastTime:%f,arrivalTime:%f\n",lastTime,trace[x].arrivalTime)
		lastTime = trace[x].arrivalTime


def main():
	logging.basicConfig(level=logging.CRITICAL, format='%(message)s')
	customTrace=[cfg.Task(0, 20.0, 3.0, 0), cfg.Task(1, 21.0, 3.0, 0.2),\
		   cfg.Task(2, 0.2, 3.0, 0.3), cfg.Task(3, 0.21, 3.0, 0.4)]
	for x in range (2,21):
		trace=[]
		cfg.paretoA = x*0.1
		migration = taskMigration.Migration(False)
		(trace,migrationMean) = migration.run()
		traceCheck(trace)
		fifo = FIFO.FIFO(trace)
		fifomean = fifo.run()
		logging.critical("====paretoA:%f, speedup:%f\n",cfg.paretoA, fifomean/migrationMean)

if __name__ == "__main__":
	main()
import os
import time

def msecond():
    return int(time.time()*1000)

def formatT(ms):
    return time.strftime("%a, %d %b %Y %H:%M:%S +0000",time.localtime(ms/1000))

class DelayRecord(object):
    delayStep = 1
    
    def __init__(self):
        self.delay_map = {}
        
    def put(self,delay):
        delayBucket = (delay/DelayRecord.delayStep)+1
        
        if self.delay_map.has_key(delayBucket):
            self.delay_map[delayBucket]+=1
        else:
            self.delay_map[delayBucket] = 1
            
    def __str__(self):
        return os.linesep.join(["%s %s"%(k,v) for k,v in self.delay_map.items()])
    
class Record(object):
    MAX_DELAY = 90*60000
    INTERVAL = 120000
    START_TIME = 0
    delayStep = 1
    TOTAL_OBJ_NUM = 0
    TOTAL_MAX_DELAY = 0
    TOTAL_MIN_DELAY = MAX_DELAY
    
    def __init__(self,bucket):
        self.minDelay = Record.MAX_DELAY
        self.maxDelay = 0
        self.bucket = bucket
        self.objNum = 0
        self.totalAvg = 0
        self.bucketAvg = 0       
        self.totalMinDelay = Record.TOTAL_MIN_DELAY
        self.totalMaxDelay = Record.TOTAL_MAX_DELAY  
        self.totalNum = 0
        self.reDelay = 0      
        
    def put(self,delay):
        self.objNum += 1
        Record.TOTAL_OBJ_NUM += 1
        
        if delay>self.maxDelay:
            self.maxDelay = delay
        
        if delay<self.minDelay:
            self.minDelay = delay
            
        self.reDelay += delay  
            
        #print self
        
    def finish(self):
        self.totalNum = Record.TOTAL_OBJ_NUM

        if self.maxDelay>Record.TOTAL_MAX_DELAY:
            Record.TOTAL_MAX_DELAY = self.maxDelay
            
        if self.minDelay<Record.TOTAL_MIN_DELAY:
            Record.TOTAL_MIN_DELAY = self.minDelay        
        
        self.totalMinDelay = Record.TOTAL_MIN_DELAY
        self.totalMaxDelay = Record.TOTAL_MAX_DELAY  
        
        deltaTime = self.bucket-Record.START_TIME
        if deltaTime <1:
            self.totalAvg = 0
            self.bucketAvg = 0
        else:
            self.totalAvg = float(self.totalNum)*1000/deltaTime   
            if deltaTime>Record.INTERVAL:
                deltaTime = Record.INTERVAL
            self.bucketAvg = float(self.objNum)*1000/deltaTime       
        
    def __str__(self):
        return "%s %s %s %s %s %s %s %s %s %s"%(
                                 self.bucket,
                                 self.objNum,
                                 self.minDelay,
                                 self.maxDelay,
                                 self.reDelay,
                                 self.totalNum,
                                 self.totalMinDelay,
                                 self.totalMaxDelay,
                                 self.bucketAvg,                               
                                 self.totalAvg)
        
class StatClient(object):

    def __init__(self, output="./stat",confName=None,run = True):
        self.run = run
        if not self.run: return
            
        self.startTime = msecond()
        self.interval = 2*60*1000 # 2 min
        Record.INTERVAL = self.interval
        Record.START_TIME = self.startTime
        self.flushNum = 5 # 10 min

        self.outfname = output
        dirName = os.path.dirname(self.outfname)
       
        if not os.path.exists(dirName):
            os.makedirs(dirName)
        self.fp = open(self.outfname+"_th.txt","wb+")
        self.fdelay = open(self.outfname+"_delay.txt","wb+")
        #self.fdelayAvg = open(self.outfname+"_delay_re.txt","wb+")
        self.delay_map = {}
        DelayRecord.delayStep = 20 # 20ms

        self.curRecord = Record(self.getBucket(msecond()))
        self.curDelayRecord = DelayRecord()
        self.cacheRecords = []  
        self.delayRecords = []
        
    def getBucket(self,t):
        return t + self.interval - (t%self.interval)
        
    def beginOp(self,ms=None):
        if not self.run: return
	if ms:
	    self.curStart = ms
	else:
	    self.curStart = msecond()
    
    def endOp(self,ms=None):
        if not self.run: return
	if ms:
	    curEnd = ms
	else:
	    curEnd = msecond()    
        delay = curEnd - self.curStart
        bucket = self.getBucket(curEnd)
        
        if bucket!=self.curRecord.bucket:
            self.curRecord.finish()
            #print self.curRecord
            self.cacheRecords.append(self.curRecord)
            self.delayRecords.append(self.curDelayRecord)
              
            self.roll()
            
            self.curRecord = Record(bucket)
            self.curDelayRecord = DelayRecord()

        self.curRecord.put(delay)
        self.curDelayRecord.put(delay)
        
    def roll(self, force=False):
        if not self.run: return        
        if not self.cacheRecords: return
        if force or len(self.cacheRecords)>=self.flushNum:
            """print "---------Rolling Start -------"
            for record in self.cacheRecords:
                print record
            print "---------Rolling End -----------"
            """
            self.cacheRecords.append("")
            self.delayRecords.append("")
 
            if self.cacheRecords:
                self.fp.write(os.linesep.join([str(r) for r in self.cacheRecords]))
                self.fdelay.write(os.linesep.join([str(r) for r in self.delayRecords]))
                self.fp.flush()
                self.fdelay.flush()
                self.cacheRecords = []
                self.delayRecords = []
    
    def close(self):
        if not self.run: return        
        self.curRecord.finish()
        self.cacheRecords.append(self.curRecord)
        self.delayRecords.append(self.curDelayRecord)       
        self.roll(True)
        self.fp.close()
        self.fdelay.close()
    
if __name__=="__main__":
    s = StatClient(confName="./conf.txt")
    for i in range(60):
        s.beginOp()
        time.sleep(1)        
        s.endOp()

    s.roll(True)
    s.close()


    
    

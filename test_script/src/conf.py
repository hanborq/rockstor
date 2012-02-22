import os
import sys

class Conf(object):
    inst = None
    def __init__(self):
        self.servAddr = "10.24.1.41:9988"
        self.statInterval = 10 #seconds
        self.flushBatch = 6 # 1 min
        self.output = ""
        self.delayStep = 20 # 20 ms
    
    def init(self, fname=None):
        if not fname or not os.path.exists(fname) or not os.path.isfile(fname):
            #print "conf file not exist!"
            return
        print "init conf"
        with open(fname,"rb") as fin:
            for line in fin.readlines():
                if not line: continue
                line = line.strip()
                if not line: continue
                if line.startswith("serv:"):
                    self.servAddr = line[len("serv:"):]
                if line.startswith("interval:"):
                    self.statInterval = int(line[len("interval:"):])
                if line.startswith("output:"):
                    self.output = line[len("output:"):]
                if line.startswith("flushNum:"):
                    self.flushBatch = int(line[len("flushNum:"):])
                
                if line.startswith("delayStep:"):
                    self.delayStep  = int(line[len("delayStep:"):])

    @staticmethod
    def instance():
        if not Conf.inst:
            Conf.inst = Conf()
        return Conf.inst
    
    def getServAddr(self):
        return self.servAddr
    
    def getInterval(self):
        return self.statInterval
    
    def __str__(self):
        return "[conf: servaddr="+self.servAddr+", interval="+str(self.statInterval)\
               +", output="+self.output+"]"
    
    
    
if __name__=="__main__":
    conf = Conf.instance()
    print sys.argv
    if len(sys.argv)>=2:
        conf.init(sys.argv[1])
    print conf
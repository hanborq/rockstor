import os
import sys
from time import time, ctime
import socket
from multiprocessing import Process, freeze_support, Pool

"""
src file format :
obj_id result obj_size starttime endtime delay
obj_id: str
result: 1 or 0. 1 succ, 0 failed
obj_size: bytes
starttime: long, ms
endtime  : long, ms
delay:     long, endtime-starttime

eg:
/XXXX_1_0036/XXXX_1_0036_00003766       1       5120    1292307288735   1292307288859   124

generator three result file:
1, result:
    summary of the test
2, throughput:
    show throuhput of every second
    output format:
           second  throughputnum
3, delay:
    show numbers of each delay num
    output format:
          ms      num
"""

usage = """
        only support python version 2.6+.

        python mergeFile.py file srcDir dstDir [fileNum]
        or
        python mergeFile.py result srcDir dstDir
        
        eg:
        python mergeFile.py file ./test/result/ final1/ [20]
          merge all (or 20) original files in ./test/result/, and write output files to final1/
        
        python mergeFile.py result final1/ final/
          merge all merged result in final1, and write output files to final
        
        original file record format :
        obj_id result obj_size starttime endtime delay
        
        obj_id: str
        result: 1 or 0. 1 succ, 0 failed
        obj_size: bytes
        starttime: long, ms
        endtime  : long, ms
        delay:     long, endtime-starttime
        
        eg:
        /XXXX_1_0036/XXXX_1_0036_00003766       1       5120    1292307288735   1292307288859   124
        
        generator three result file:
        1, result:
            summary of the test
        2, throughput:
            show throuhput of every second
            output format:
                   second  throughputnum
        3, delay:
            show numbers of each delay num
            output format:
                  ms      num

        """
def printUsage():
    global usage
    print usage

class Utils:
    @staticmethod
    def mkdirs(p):
        if not os.path.exists(p):
            os.makedirs(p)

    @staticmethod
    def getTime():
        return long(time()*1000)

    @staticmethod
    def diff(startTime, endTime):
        if startTime == endTime:
            return 1.0
        return float(endTime - startTime) / 1000

    @staticmethod
    def gethostname():
        return socket.gethostname()

    @staticmethod
    def delDir(dirPath, delSelf = False):
        if not os.path.exists(dirPath):
            return True
        if not os.path.isdir(dirPath):
            return True
        files = [os.path.join(dirPath, fName) for fName in os.listdir(dirPath)]
        for fPath in files:
            if not os.path.isdir(fPath):
                os.remove(fPath)
            else:
                delDir(fPath, True)
        if delSelf:
            os.rmdir(dirPath)
        return True

class ResultPath:
    SEP_CHAR = "_"
    def __init__(self, dirName, seq = None):
        self.rootDir = dirName
        self.setSeq(seq)

    def getRootDir(self):
        return self.rootDir

    def setSeq(self, seq = None):
        if not seq:
            self.seq = ResultPath.SEP_CHAR.join([str(Utils.getTime()), Utils.gethostname(), str(os.getpid())])
        else:
            self.seq = seq

    def getDelayPath(self):
        return os.path.join(self.rootDir, self.seq + ".delay")

    def getResultPath(self):
        return os.path.join(self.rootDir, self.seq + ".result")

    def getTpPath(self):
        return os.path.join(self.rootDir, self.seq + ".throughput")

    def good(self):
        if not os.path.isfile(self.getDelayPath()):
            return False
        if not os.path.isfile(self.getResultPath()):
            return False
        if not os.path.isfile(self.getTpPath()):
            return False
        return True

    @staticmethod
    def parsePath(fPath):
        dirName = os.path.dirname(fPath)
        sep = os.path.splitext(os.path.basename(fPath))[0]

        if not sep:
            return None

        rp = ResultPath(dirName, sep)
        return rp

def parseObjectFile(fname):
    # seconds -> good num
    delayCounter = {}
    # second -> [succ, bad]
    tpCounter = {}

    retStartTime = Utils.getTime()
    retEndTime = 0
    succNum = 0
    retSize = 0

    for i in range(1):
        if not os.path.isfile(fname):
            break

        with open(fname, "r") as fp:
            while True:
                line = fp.readline()
                if not line: break

                items = line.split("\t")
                item_num = len(items)
                if not items or not (item_num == 5 or item_num == 6): continue

                if item_num == 6:
                    [objID, succ, size, startTime, stopTime, delay] = items
                else:
                    [objID, succ, size, startTime, stopTime] = items

                startTime = startTime.strip()
                stopTime = stopTime.strip()
                #delay = delay.strip()

                if not startTime or not stopTime or stopTime < startTime:
        			continue

                startTime = long(startTime)
                stopTime = long(stopTime)
                delay = stopTime - startTime
                tpTime = stopTime / 1000

                if long(succ):
                    if tpCounter.has_key(tpTime):
                        tpCounter[tpTime][0] += 1
                    else:
                        tpCounter[tpTime] = [1, 0]

                    if delayCounter.has_key(delay):
                        delayCounter[delay] += 1
                    else:
                        delayCounter[delay] = 1

                    if retStartTime > startTime:
                        retStartTime = startTime
                    if retEndTime < stopTime:
                        retEndTime = stopTime
                    retSize += long(size)
                else:
                    if tpCounter.has_key(tpTime):
                        tpCounter[tpTime][1] += 1
                    else:
                        tpCounter[tpTime] = [0, 1]

    return [delayCounter, tpCounter, retStartTime, retEndTime, retSize]

def calcResult(resultSet, dstDir):
    retDelayCounter = {}
    retTpCounter = {}
    retStartTime = Utils.getTime()
    retEndTime = 0
    retSuccNum = 0
    retTotalNum = 0
    retSize = 0
    retDelayTime = 0

    Utils.delDir(dstDir)

    for result in resultSet:
        [delayCounter, tpCounter, startTime, endTime, size] = result
        if delayCounter:
            for (k, v) in delayCounter.items():
                if retDelayCounter.has_key(k):
                    retDelayCounter[k] += v
                else:
                    retDelayCounter[k] = v

                retSuccNum += v
                retDelayTime += k * v
        if tpCounter:
            for (k, v) in tpCounter.items():
                if retTpCounter.has_key(k):
                    retTpCounter[k][0] += v[0]
                    retTpCounter[k][1] += v[1]
                else:
                    retTpCounter[k] = v
                retTotalNum += v[0] + v[1]
        if retStartTime > startTime:
            retStartTime = startTime
        if retEndTime < endTime:
            retEndTime = endTime

        retSize += size

    wasteTime = Utils.diff(retStartTime, retEndTime)

    avegNum = retSuccNum / wasteTime
    avegBytes = (retSize / wasteTime) / (1 << 10)

    retDelayCounter = retDelayCounter.items()
    retDelayCounter.sort()

    retTpCounter = retTpCounter.items()
    retTpCounter.sort()

    minDelay = 0
    maxDelay = 0

    if retDelayCounter:
        minDelay = retDelayCounter[0][0]
        maxDelay = retDelayCounter[-1][0]

    if not retSuccNum:
        retSuccNum = 1

    retStr = os.linesep.join(["%s\t%s\t%s" % (retStartTime, retEndTime, retSize),
                              "Total       Num: %.4f w" % (retTotalNum / 10000.0),
                              "Succ        Num: %.4f w" % (retSuccNum / 10000.0),
                              "Succ Write Byte: %.4f MB" % (retSize / float(1 << 20)),
                              "Start      Time: %s" % (ctime(retStartTime / 1000)),
                              "Stop       Time: %s" % (ctime(retEndTime / 1000)),
                              "Waste      Time: %f seconds" % (wasteTime),
                              "Average     Num: %f /sec" % (avegNum),
                              "Average   Bytes: %f KB/sec" % (avegBytes),
                              "Min       Delay: %d msec" % (minDelay),
                              "Max       Delay: %d msec" % (maxDelay),
                              "Average   Delay: %d msec" % (retDelayTime / retSuccNum),
                              ""])
    print "*" * 20
    print retStr
    print "*" * 20

    rp = ResultPath(dstDir)
    with open(rp.getResultPath(), "wb+") as fOut:
        fOut.write(retStr)
    print "write", rp.getResultPath(), "finished"

    # write result
    #if retDelayCounter:
    fmt_str = "%%d\t%%d%s" % os.linesep
    with open(rp.getDelayPath(), "wb+") as fOut:
        for item in retDelayCounter:
            fOut.write(fmt_str % (item))
    print "write", rp.getDelayPath(), "finished"

    #if retTpCounter:
    fmt_str = "%%d\t%%d\t%%d%s" % os.linesep
    with open(rp.getTpPath() , "wb+") as fOut:
        for item in retTpCounter:
            fOut.write(fmt_str % (item[0], item[1][0], item[1][1]))

    print "write", rp.getTpPath(), "finished"
    print "write finished"
    print "OK"
    print
    print

def mergeObjectFile(srcDir, dstDir, fileNum = 0):
    if not os.path.isdir(srcDir):
        print "src dir", srcDir, "not exists"
        return False

    files = os.listdir(srcDir)

    if fileNum:
        files = [fName for fName in ["%03d" % (idx,) for idx in range(1, fileNum + 1)] if fName in files]
    files = [os.path.join(srcDir, fName) for fName in files]


    proc_num = len(files)
    if proc_num > 8:
        proc_num = 8
    pool = Pool(processes = proc_num)
    print "start", proc_num, "processes pool"
    resultSet = [x.get() for x in [pool.apply_async(parseObjectFile, (fName,)) for fName in files]]

    print "parse", len(files), "files finished!"
    pool.close()

    if not resultSet:
        print "read src dir(", srcDir, ") failed"
        printUsage()
        return
    calcResult(resultSet, dstDir)

def parseResult(rp):
    #read result
    retStartTime = Utils.getTime()
    retEndTime = 0
    retSize = 0
    with open(rp.getResultPath(), "r") as fIn:
        line = fIn.readline()
        if line:
            items = line.split("\t")
            if len(items) == 3:
                [retStartTime, retEndTime, retSize] = [long(item.strip()) for item in items]

    retDelayCounter = {}
    retTpCounter = {}

    #parse delay
    with open(rp.getDelayPath(), "r") as fIn:
        while True:
            line = fIn.readline()
            if not line:
                break
            items = line.split("\t")
            if len(items) == 2:
                [k, v] = [long(item.strip()) for item in items]
                retDelayCounter[k] = v

    #parse tp
    with open(rp.getTpPath(), "r") as fIn:
        while True:
            line = fIn.readline()
            if not line:
                break
            items = line.split("\t")
            if len(items) == 3:
                [k, v0, v1] = [long(item.strip()) for item in items]
                retTpCounter[k] = [v0, v1]

    return [retDelayCounter, retTpCounter, retStartTime, retEndTime, retSize]

def mergeResult(srcDir, dstDir):
    if not os.path.isdir(srcDir):
        print "src dir", srcDir, "is not exist"
        return False
    if not os.path.exists(dstDir):
        os.makedirs(dstDir)
    import glob
    delayPaths = glob.glob(os.path.join(srcDir, "*.delay"))
    if not delayPaths:
        print "src dir", srcDir, "contains no result file"
        return  False
    rps = [rp for rp in [ResultPath.parsePath(fPath) for fPath in delayPaths] if rp.good()]

    proc_num = len(rps)
    if proc_num > 8:
        proc_num = 8
    pool = Pool(processes = proc_num)
    print "start", proc_num, "processes pool"
    resultSet = [x.get() for x in [pool.apply_async(parseResult, (rp,)) for rp in rps]]

    print "parse", len(rps), "result finished!"
    pool.close()
    if not resultSet:
        print "read src dir(", srcDir, ") failed"
        printUsage()
        return
    calcResult(resultSet, dstDir)

def testResultPath():
    rp = ResultPath("./a/")
    print rp.getDelayPath()
    print rp.getResultPath()
    print rp.getTpPath()

    print "-" * 20
    rp2 = ResultPath.parsePath(rp.getDelayPath())
    print rp2.getDelayPath()
    print rp2.getResultPath()
    print rp2.getTpPath()

def run():
    argc = len(sys.argv)
    if argc != 4 and argc != 5:
        printUsage()
        return

    if argc == 4:
        sys.argv.append("0")

    [pyFile, op, srcDir, dstDir, num] = sys.argv

    try:
        num = long(num)
        freeze_support()
        if op == "file":
            mergeObjectFile(srcDir, dstDir, num)
        elif op == "result":
            mergeResult(srcDir, dstDir)
        else:
            print "merge type should be file or result!"
            printUsage()
            return
    except Exception, e:
        print e
        #printUsage()
        return

if __name__ == "__main__":
    run()



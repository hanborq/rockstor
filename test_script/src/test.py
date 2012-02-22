import os
import sys
from multiprocessing import Process, freeze_support, Pool
from time import sleep, time
from pebble import *

opMap = None

def showHelp(op = None):
    global opMap
    descPrefix = "python ./test.py"
    print "usage:"
    if op:
        v = opMap[op]
        print descPrefix, op, v['desc']
    else:
        for (k, v) in opMap.items():
            print descPrefix, k, v['desc']

class Partition:
    MIN_SEG_NUM = 4
    SEG_NUM = 64
    CHAR_NUM = 2
    MAX_VALUE = 1 << (CHAR_NUM * 4)
    ITEMS = ["%02x" % (c,) for c in range(0, MAX_VALUE, MAX_VALUE / SEG_NUM)]
    GAP = SEG_NUM / MIN_SEG_NUM
    PREFIXS = [ITEMS[((i * GAP) % SEG_NUM) + (i / MIN_SEG_NUM)] for i in range(SEG_NUM)]
    print GAP

    @staticmethod
    def show():
        for i in range(Partition.SEG_NUM):
            if i % Partition.MIN_SEG_NUM == 0:
                print
            print Partition.PREFIXS[i],
        print

    @staticmethod
    def get(idx):
        return Partition.PREFIXS[idx % Partition.SEG_NUM]

    @staticmethod
    def genBucketPrefix(pebbleIdPrefix, procNum):
        pebbleFmt = "%%s_%s_%%04d" % (pebbleIdPrefix)
        return [pebbleFmt % (Partition.PREFIXS[i % Partition.SEG_NUM], i) for i in range(procNum)]

#def testBatchRandomPut(url, minSize, maxSize, idPrefix="pebble",num=10000,randomID=True, resultId=None,logId=None):        
def BatchRandomPutStart():
    try:
        [url, pebbleIdPrefix, minSize, maxSize, procNum, pebbleNum, idSeqType] = sys.argv
        minSize = long(minSize) << 10
        maxSize = long(maxSize) << 10

        assert(minSize <= maxSize)

        procNum = int(procNum)
        pebbleNum = int(pebbleNum)
        idSeqType = (idSeqType == '1')

        print "Cmd Param:"
        print "PebbleID Prefix:", pebbleIdPrefix
        print "pebble Size Range: %d - %d KB" % (minSize >> 10, maxSize >> 10)
        print "proc        Num:", procNum
        print "pebble pre proc:", pebbleNum
        print

        pebblePrefix = Partition.genBucketPrefix(pebbleIdPrefix, procNum)

        #url, size, idPrefix="pebble",num=10000, randomID=True, resultFile=None        
        #def testBatchFixPut(url, size, idPrefix="pebble",num=10000, randomID=True, resultFile=None)
        print "init params"

        params = [(url, minSize, maxSize, pebblePrefix[i], pebbleNum, idSeqType) for i in range(procNum)]

        pool = Pool(processes = procNum)
        print "start pool"
        resultSet = [x.get() for x in [pool.apply_async(testBatchRandomPut, param) for param in params]]
        print "pebble op finished"

    except Exception, e:
        print e,e.args,e.mro

def genBucketPrefix(pebbleIdPrefix, procNum):
    pebbleFmt = "%s_%%04d" % (pebbleIdPrefix)
    return [pebbleFmt % (i) for i in range(procNum)]

#def testBatchFixPut(url, size, idPrefix="pebble",num=10000, randomID=True, resultId=None,logId=None):         
def BatchFixPutStart():
    try:
        [url, pebbleIdPrefix, size, procNum, pebbleNum, idSeqType] = sys.argv
        size = long(size) << 10
        procNum = int(procNum)
        pebbleNum = int(pebbleNum)
        idSeqType = (idSeqType == '1')

        print "Cmd Param:"
        print "URL            :", url
        print "PebbleID Prefix:", pebbleIdPrefix
        print "pebble     Size: %d KB" % (size >> 10,)
        print "proc        Num:", procNum
        print "pebble pre proc:", pebbleNum
        print "ID Random  :", idSeqType
        print

        pebblePrefix = Partition.genBucketPrefix(pebbleIdPrefix, procNum)

        #url, size, idPrefix="pebble",num=10000, randomID=True, resultFile=None        
        #def testBatchFixPut(url, size, idPrefix="pebble",num=10000, randomID=True, resultFile=None)
        print "init params"

        params = [(url, size, pebblePrefix[i], pebbleNum, idSeqType) for i in range(procNum)]
        pool = Pool(processes = procNum)
        print "start pool"
        resultSet = [x.get() for x in [pool.apply_async(testBatchFixPut, param) for param in params]]
        print "pebble op finished"

    except Exception, e:
        print e,e.args


#def testBatchGet(url, idPrefix="pebble",num=10000,randomID=True, resultId=None,logId=None)            
def BatchGetStart():
    try:
        [url, pebbleIdPrefix, procNum, pebbleNum, idSeqType] = sys.argv
        procNum = int(procNum)
        pebbleNum = int(pebbleNum)
        idSeqType = (idSeqType == '1')

        print "Cmd Param:"
        print "PebbleID Prefix:", pebbleIdPrefix
        print "proc        Num:", procNum
        print "pebble pre proc:", pebbleNum
        print "ID Random  :", idSeqType
        print

        pebblePrefix = Partition.genBucketPrefix(pebbleIdPrefix, procNum)

        print "init params"

        params = [(url, pebblePrefix[i], pebbleNum, idSeqType) for i in range(procNum)]

        pool = Pool(processes = procNum)
        print "start pool"
        resultSet = [x.get() for x in [pool.apply_async(testBatchGet, param) for param in params]]

        print "pebble op finished!"
    except Exception, e:
        print e

#def testBatchDel(url, idPrefix="pebble",num=10000,randomID=True, resultId=None,logId=None):         
def BatchDelStart():
    try:
        [url, pebbleIdPrefix, procNum, pebbleNum, idSeqType] = sys.argv
        procNum = int(procNum)
        pebbleNum = int(pebbleNum)
        idSeqType = (idSeqType == '1')

        print "Cmd Param:"
        print "PebbleID Prefix:", pebbleIdPrefix
        print "proc        Num:", procNum
        print "pebble pre proc:", pebbleNum
        print "ID Random  :", idSeqType
        print

        pebblePrefix = Partition.genBucketPrefix(pebbleIdPrefix, procNum)

        #url, size, idPrefix="pebble",num=10000, randomID=True, resultFile=None        
        #def testBatchFixPut(url, size, idPrefix="pebble",num=10000, randomID=True, resultFile=None)
        print "init params"

        params = [(url,  pebblePrefix[i], pebbleNum, idSeqType) for i in range(procNum)]

        pool = Pool(processes = procNum)
        print "start pool"
        resultSet = [x.get() for x in [pool.apply_async(testBatchDel, param) for param in params]]

        print "pebble op finished!"

    except Exception, e:
        print e


def PutFileStart():
    pass

def GetFileStart():
    pass

def DelFileStart():
    pass

def PutDirStart():
    pass



opMap = {"BatchFixPut":{"argNum":6,
                        "desc": "url pebbleIdPrefix pebbleSize(KB) procNum pebbleNum randomID(1/0)",
                        "startFunc":BatchFixPutStart},
         "BatchRandomPut":{"argNum":7,
                                 "desc": "url pebbleIdPrefix pebbleMinSize(KB) pebbleMaxSize(KB) procNum pebbleNum randomID(1/0)",
                                 "startFunc":BatchRandomPutStart},
         "BatchGet":{"argNum":5,
                        "desc": "url pebbleIdPrefix procNum pebbleNum randomID(1/0)",
                        "startFunc":BatchGetStart},
         "BatchDel":{"argNum":5,
                        "desc": "url pebbleIdPrefix procNum pebbleNum randomID(1/0)",
                        "startFunc":BatchDelStart},
         "help":{"argNum":0,
                 "desc":"",
                 "handler":showHelp,
                 "startFunc":showHelp}
         }

def run():
    freeze_support()
    global opMap

    

    if len(sys.argv) < 3:
        showHelp()
        return
    Pebble.hdfsRoot = sys.argv[1]
    opStr = sys.argv[2]
    sys.argv = sys.argv[3:]

    if not opMap.has_key(opStr):
        showHelp()
        return

    opInfo = opMap[opStr]

    #print len(sys.argv),sys.argv
    #print opInfo["argNum"] 
    if opInfo["argNum"] != len(sys.argv):
        showHelp(opStr)
        return

    startFunc = opInfo["startFunc"]
    startFunc()



if __name__ == '__main__':
    run()
    #testMulti()
    #print Partition.genBucketPrefix("pebble", 40)


import os
from httplib import *
from random import random
from random import shuffle
import logging
from time import time
from time import asctime
import socket
import httplib
import hashlib
from statCalc import StatClient
httplib.HTTPConnection.debuglevel=0

def initLog(fname = None, level = logging.CRITICAL):
    logging.basicConfig(level = level,
                        format = '%(asctime)s %(levelname)s %(message)s',
                        filename = fname,
                        filemode = 'w')

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

    @staticmethod
    def getMD5(data):
        md5 = hashlib.md5()
        md5.update(data)
        return md5.hexdigest()
    
    @staticmethod
    def getSignHeader(method,date,
        secureKey= "3833eef7745a416573c3508ee24c71ecc5f671e5", accessKey="084f1fcd2acadb201751"):
	"""
	Authorization : ROCK1 accessKey:signature
	signature=MD5(METHOD+Date+SecurityKey);
	"""
	signature = Utils.getMD5("".join([method,date,secureKey]))
	
	return "Authorization : ROCK1 "+accessKey+":"+signature
    
    @staticmethod
    def signHeader(header,method="PUT",secureKey= "3833eef7745a416573c3508ee24c71ecc5f671e5", accessKey="084f1fcd2acadb201751"):
	#header["Authorization"] = "ROCK1 "+accessKey+":"+Utils.getMD5("".join([method,header["Date"],secureKey]))
	return header

class DataPool:
    data = ""
    sizeAlign = 1 << 20 # 1 MB
    sizeMask = sizeAlign - 1
    maxSize = 0
    @staticmethod
    def getData(reqSize):
        size = ((reqSize + DataPool.sizeMask) & ~DataPool.sizeMask)

        if reqSize > DataPool.maxSize:
            del DataPool.data
            DataPool.data = "0123456789abcdef" * (size / 16)
            DataPool.maxSize = size
        return DataPool.data[0:reqSize]

    @staticmethod
    def clear():
        del DataPool.data
        DataPool.maxSize = 0

class Bucket:
    hdfsRoot = "/rockstor"
    def __init__(self, bucketID = None, owner = "cestbon"):
        self.owner = owner
        self.bucketID = bucketID

    def __str__(self):
        return "[bucket: %s/%s]" % (self.owner, self.bucketID)

    def getURI(self):
        return Pebble.hdfsRoot + "/" + self.bucketID

    def put(self, url, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner), "Date": asctime(), "Content-length":"0"}
        conn = None

        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "PUT", url = self.getURI(), headers = Utils.signHeader(headers))
            #print self.getURI()
            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["PUT", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()
                return True
            else:
                rsp_data = response.read()
                logging.error(" ".join(["PUT", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "Failed, exception:", str(e)]))
        if conn:
            conn.close()
            return False

    def get(self, url, prefix = None, delimiter = None, marker = None, maxKeys = 0, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner), "Date": asctime(), "Content-length":"0"}
        queryList = []
        if prefix:
            #?prefix=t&marker=test&max-keys=25
            queryList.append("prefix=" + prefix)

        if delimiter:
            queryList.append("delimiter=" + delimiter)

        if marker:
            queryList.append("marker=" + marker)
        if maxKeys:
            queryList.append("max-keys=" + str(maxKeys))

        queryStr = ""
        if queryList:
            queryStr = "?" + "&".join(queryList)

        print queryStr
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "GET", url = self.getURI() + queryStr, headers = Utils.signHeader(headers,"GET"))
            #conn.request(method="GET",url="/tomcat.gif") #,headers=headers)

            response = conn.getresponse()

            if response.status == 200:
                #dataLen = long(response.getheader("Content-Length"))

                rsp_data = response.read()
                conn.close()
                conn = None

                print rsp_data

                logging.info(" ".join(["GET", str(self), "OK"]))

                return rsp_data
            else:
                rsp_data = response.read()
                logging.error(" ".join(["GET", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["GET", str(self), "Failed, exception:", str(e)]))
        if conn:
            conn.close()
        return None

    def getACL(self, url, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner), "Date": asctime(), "Content-length":"0"}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "GET", url = self.getURI() + "?acl", headers = Utils.signHeader(headers,"GET"))
            #conn.request(method="GET",url="/tomcat.gif") #,headers=headers)

            response = conn.getresponse()

            if response.status == 200:
                #dataLen = long(response.getheader("Content-Length"))

                rsp_data = response.read()
                conn.close()
                conn = None

                print rsp_data

                logging.info(" ".join(["GET", str(self), "OK"]))

                return rsp_data
            else:
                rsp_data = response.read()
                logging.error(" ".join(["GET", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["GET", str(self), "Failed, exception:", str(e)]))
        if conn:
            conn.close()
        return None

    def drop(self, url, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner), "Date": asctime(), "Content-length":"0"}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "DELETE", url = self.getURI(), headers = Utils.signHeader(headers,"DELETE"))
            #conn.request(method="GET",url="/tomcat.gif") #,headers=headers)

            response = conn.getresponse()

            if response.status == 204:
                return True

            if response.status == 200:
                rsp_data = response.read()
                conn.close()
                conn = None

                print rsp_data

                logging.info(" ".join(["DELETE", str(self), "OK"]))

                return False
            else:
                rsp_data = response.read()
                logging.error(" ".join(["DELETE", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["DELETE", str(self), "Failed, exception:", str(e)]))
        if conn:
            conn.close()
        return False


class Pebble:
    hdfsRoot = "/rockstor"
    def __init__(self, pebbleID, version = 0, owner = "cestbon"):
        self.pebbleID = pebbleID
        self.version = version
        self.owner = owner

    def __str__(self):
        return "[Pebble: id=%s, version=%s, owner=%s]" % (self.pebbleID,
                                              self.version, self.owner)
    def getURI(self):
        #print Pebble.hdfsRoot
        #return os.path.join(Pebble.hdfsRoot, "." + self.pebbleID)
        return Pebble.hdfsRoot + self.pebbleID

    def putFile(self, url, srcFile, timeout = 60 * 1000 * 5):
        if not os.path.exists(srcFile) or not os.path.isfile(srcFile):
            logging.error(" ".join(["PUT", str(self), "(file:", srcFile, ") Failed, reason: file not exist or not regular file!"]))
            return False

        try:
            fIn = open(srcFile, "rb")
            data = fIn.read()
            fIn.close()
            if self.put(url, data):
                logging.info(" ".join(["PUT", str(self), "(file:", srcFile, ") OK"]))
                return True
            logging.error(" ".join(["PUT", str(self), "(file:", srcFile, ") Failed"]))


        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "(file:", srcFile, ") Failed, exception:", str(e)]))

        return False

    def getFile(self, url, dstFile, timeout = 60 * 1000 * 5):
        try:
            data = self.get(url)
            if not data:
                logging.error(" ".join(["GET", str(self), "(file:", srcFile, ") Failed, reason: download data failed"]))
                return False

            fOut = open(srcFile, "wb+")
            fOut.write(data)
            fOut.close()

            logging.info(" ".join(["PUT", str(self), "(file:", srcFile, ") OK"]))
            return True
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "(file:", srcFile, ") Failed, exception:", str(e)]))

        return False


    def put(self, url, data, timeout = 60 * 1000 * 5): #1min
        assert(data)
        headers = {"Content-Type":"application/octet-stream",
           "Content-Length":str(len(data)),
           "x-rock-meta-time":str(time()),
           "x-rock-meta-coder":"terry",
           "Authorization":"ROCK0 %s:uselesschars" % (self.owner),
           "Date": asctime()}

        conn = None

        try:
            #print self.getURI()
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "PUT", url = self.getURI(), body = data, headers = Utils.signHeader(headers,"PUT"))

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["PUT", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()
                return True
            else:
                rsp_data = response.read()
                logging.error(" ".join(["PUT", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False

    def drop(self, url, version = 0, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner),
                   "Date": asctime(),
                   "Content-Length":"0"}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "DELETE", url = self.getURI(), headers = Utils.signHeader(headers,"DELETE"))

            response = conn.getresponse()

            if response.status == 204:
                rsp_data = response.read()
                logging.info(" ".join(["DROP", str(self), "OK, Result:"]))
                logging.info(rsp_data)
                conn.close()
                return True
            else:
                rsp_data = response.read()
                logging.error(" ".join(["DROP", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["DROP", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False

    def head(self, url, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner),
                   "Date": asctime(),
                   "Content-Length":"0"}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "HEAD", url = self.getURI(), headers = Utils.signHeader(headers,"HEAD"))
            #conn.request(method="GET",url="/tomcat.gif") #,headers=headers)

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()

                conn.close()
                conn = None

                logging.info(" ".join(["HEAD", str(self), "OK"]))
                return rsp_data
            else:
		rsp_data = response.read()
                logging.error(" ".join(["HEAD", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)].extend(response.getheaders())))
        except Exception, e:
            logging.error(" ".join(["HEAD", str(self), "Failed, exception:", str(e)]))
        if conn:
            conn.close()
        return None

    def get(self, url, version = 0, checkdata = False, timeout = 60 * 1000 * 5):

        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner),
                   "Date": asctime(),
                   "Content-Length":"0"}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "GET", url = self.getURI(), headers = Utils.signHeader(headers,"GET"))
            #conn.request(method="GET",url="/tomcat.gif") #,headers=headers)

            response = conn.getresponse()
            rsp_data = response.read()
            if response.status == 200:
                #print response.getheaders()
                dataLen = long(response.getheader("Content-Length"))


                conn.close()
                conn = None

                if len(rsp_data) != dataLen:
                    logging.error(" ".join(["GET", str(self), "Failed, reason: data len error (", len(rsp_data), ",", dataLen, ")"]))
                    return None

                if checkdata and DataPool.getData(dataLen) != rsp_data:
                    logging.error(" ".join(["GET", str(self), "Failed, reason: data error"]))
                    return None

                logging.info(" ".join(["GET", str(self), "OK, data size:", str(dataLen)]))

                return rsp_data
            else:
		rsp_data = response.read()		
                logging.error(" ".join(["GET", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["GET", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return None

def genBucket(idPrefix, url, write = True):
    bucketID = idPrefix
    if write:
        bucket = Bucket(idPrefix)
        bucket.drop(url)
        bucket.put(url)
        logging.info("put bucket" + str(bucket))
    return "/" + idPrefix + "/" + idPrefix


class IDGen:
    def __init__(self, num, rand = False):
        self.rand = rand
        self.num = num
        self.idx = 0

        if self.rand:
            self.randObj = random.Random(os.urandom(16))
            self.randomFunc = self.randObj.uniform
            self.func = self.__next_rand
        else:
            self.func = self.__next_seq

    def __next_rand(self):
        return int(self.randomFunc(0, self.num))

    def __next_seq(self):
        ret = self.idx
        self.idx += 1
        return ret

    def next(self):
        return self.func()


def testBatchFixPut(url, size, idPrefix = "pebble", num = 10000, randomID = True):
    
    outputName = "/home/schubert/cestbon/result/fixput/size_%s/%s"%(size,idPrefix)
    idPrefix = genBucket(idPrefix, url)
    logging.info("start proc" + str(os.getpid()))
    strFmt = "%s_%%08d" % (idPrefix,)
    i = 0

    s = StatClient(outputName)
    while i < num:
        p = Pebble(strFmt % (i))
	s.beginOp()
        p.put(url, DataPool.getData(size))
	s.endOp()
        i += 1
    s.close()
    return None

def testBatchGet(url, idPrefix = "pebble", num = 10000, randomID = True):
    import os
    outputName = "/home/schubert/cestbon/result/get/size/%s"%(idPrefix)
    idPrefix = genBucket(idPrefix, url, False)
    logging.info("start proc" + str(os.getpid()))
    strFmt = "%s_%%08d" % (idPrefix,)

    idgen = IDGen(num, randomID)

    s = StatClient(outputName)    
    i = 0
    while i < num:
        p = Pebble(strFmt % (idgen.next()))
	s.beginOp()
	data = p.get(url, checkdata = True)
	s.endOp()
        i += 1
    s.close()
    return None


def testBatchDel(url, idPrefix = "pebble", num = 10000, randomID = True):
    import os
    outputName = "/home/schubert/cestbon/result/del/size/%s"%(idPrefix)
    idPrefix = genBucket(idPrefix, url, False)
    logging.info("start proc" + str(os.getpid()))
    strFmt = "%s_%%08d" % (idPrefix,)

    idgen = IDGen(num, randomID)

    s = StatClient(outputName)
    i = 0
    while i < num:
        p = Pebble(strFmt % (idgen.next()))
        s.beginOp()
        p.drop(url)
        s.endOp()
        i += 1
    s.close()
    return None

def testBatchRandomPut(url, minSize, maxSize, idPrefix = "pebble", num = 10000, randomID = True):
    import os
    idPrefix = genBucket(idPrefix, url)

    strFmt = "%s_%%08d" % (idPrefix,)
    delta = maxSize - minSize

    i = 0
    while i < num:
        p = Pebble(strFmt % (i))
        size = minSize + long(random()*delta)
        p.put(url, DataPool.getData(size))
        i += 1

    return None

# http://10.24.1.252:8080/rockstor    
def testBase():
    p = Pebble("abd", 123)
    print p
    url = "10.24.1.41:48080"
    ret = p.put(url, DataPool.getData(1024))
    assert(ret)
    ret = p.get(url, checkdata = True)
    assert(len(ret) == 1024)
    ret = p.drop(url)
    assert(ret)
    print "finished"

def testBucketBase(bucketID, url = "10.24.1.252:8080"):
    Bucket.hdfsRoot = "/rockstor"
    bucket = Bucket(bucketID)
    bucket.put(url)
    bucket.get(url)
    bucket.getACL(url)
    #bucket.drop(url)

def testPebbleNew(pebbleID, url = "10.24.1.252:8080"):
    pebble = Pebble(pebbleID)
    pebble.put(url, DataPool.getData(4 << 10))
    data = pebble.get(url, 0, True)

from httplib import *
#httplib.HTTPConnection.debuglevel=1
#TODO: calc time, multi-thread        
if __name__ == "__main__":
    initLog()
    print asctime()
    bucketID = "terry_bucket"
    url = "10.24.1.41:48080"
    owner = "terry.xu@gmail.com"

    testBucketBase("pebble_0001", url)
    testPebbleNew("/pebble_0001/pebble_XXXX_00000001", url)
    #testPebbleNew(owner, "terry_bucket/pic/2.jpg")

    #Bucket.hdfsRoot = "/rockstor"
    #bucket = Bucket(owner, bucketID)
    #bucket.get(url, prefix = "/pic/", delimiter = "/", marker = "/pic/1.jpg", maxKeys = 1) #,



    #testResultFile()    
    #testBase()
    #def testBatchFixPut(url, size, idPrefix="pebble",num=10000, randomID=True, resultId=None)
    #print testBatchFixPut("10.24.1.252:8080",4096,"p",10,1)

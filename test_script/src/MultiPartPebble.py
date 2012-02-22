from httplib import *
import os
from random import random
from random import shuffle
import logging
from time import time
from time import asctime
import socket
import httplib
from pebble import Utils
httplib.HTTPConnection.debuglevel=0

class MultiPartPebble:
    hdfsRoot = "/rockstor"
    def __init__(self, pebbleID, version = 0, owner = "yuanfeng.zhang@gmail.com"):
        self.pebbleID = pebbleID
        self.version = version
        self.owner = owner
	self.uploadId = None
	self.parts = {}
	

    def __str__(self):
        return "[Pebble: id=%s, version=%s, owner=%s]" % (self.pebbleID,
                                              self.version, self.owner)
    def getURI(self):
        return MultiPartPebble.hdfsRoot + self.pebbleID


    def listMultiPars(self,url, timeout = 60 * 1000 * 5):
	curUrl = self.getURI()+"?uploads" 
        headers = {
           "Content-Length":"0",
           "x-rock-meta-time":str(time()),
           "x-rock-meta-coder":"terry",
           "Authorization":"ROCK0 %s:uselesschars" % (self.owner),
           "Date": asctime()}	

        conn = None

        try:
            #print self.getURI()
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "GET", url = curUrl,  headers = Utils.signHeader(headers,"GET"))

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["GET", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()
		print rsp_data
                return True
            else:
                rsp_data = response.read()
                logging.error(" ".join(["PUT", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False
	
    
    def listParts(self,url, timeout = 60 * 1000 * 5):
	curUrl = self.getURI()+"?uploadId="+self.uploadId 
        headers = {
           "Content-Length":"0",
           "x-rock-meta-time":str(time()),
           "x-rock-meta-coder":"terry",
           "Authorization":"ROCK0 %s:uselesschars" % (self.owner),
           "Date": asctime()}	

        conn = None

        try:
            #print self.getURI()
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "GET", url = curUrl,  headers = Utils.signHeader(headers,"GET"))

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["GET", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()
		print rsp_data
                return True
            else:
                rsp_data = response.read()
                logging.error(" ".join(["PUT", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False
    
    def initMultiPart(self,url, timeout = 60 * 1000 * 5): #1min
	curUrl = self.getURI()+"?uploads" 
        headers = {"Content-Type":"application/octet-stream",
           "Content-Length":"0",
           "x-rock-meta-time":str(time()),
           "x-rock-meta-coder":"terry",
           "Authorization":"ROCK0 %s:uselesschars" % (self.owner),
           "Date": asctime()}	

        conn = None

        try:
            #print self.getURI()
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "POST", url = curUrl,  headers = Utils.signHeader(headers,"POST"))

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["POST", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()
		print rsp_data
		"""
		<UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		"""
		pos = rsp_data.find("<UploadId>")
		if pos<=0:
		    return False
		pos2 = rsp_data.find("</UploadId>")
		if pos2<=pos:
		    return False
		
		uploadId = rsp_data[pos+len("<UploadId>"):pos2]
		print "uploadId:",uploadId
		self.uploadId = uploadId
                return uploadId
            else:
                rsp_data = response.read()
                logging.error(" ".join(["PUT", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False
	
    def putPart(self,url, data, partNumber=1, uploadId=None, timeout = 60 * 1000 * 5): #1min
        assert(data)
        headers = {"Content-Type":"application/octet-stream",
           "Content-Length":str(len(data)),
           "x-rock-meta-time":str(time()),
           "x-rock-meta-coder":"terry",
           "Authorization":"ROCK0 %s:uselesschars" % (self.owner),
           "Date": asctime()}

        conn = None
	
	if not uploadId:
	    uploadId = self.uploadId

        try:
            #print self.getURI()
            conn = HTTPConnection(url, timeout = timeout)
	    curUrl = self.getURI()+"?partNumber=%s&uploadId=%s"%(partNumber,uploadId)
            conn.request(method = "PUT", url = curUrl, body = data, headers = Utils.signHeader(headers,"PUT"))

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["PUT", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()

		etag = response.getheader("ETag")
		self.parts[partNumber]=etag
		print partNumber,etag
                return etag		
            else:
                rsp_data = response.read()
                logging.error(" ".join(["PUT", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["PUT", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False

    def completeMultiPart(self,url, uploadId=None, timeout = 60 * 1000 * 5):
        conn = None
	
	if not uploadId:
	    uploadId = self.uploadId

	partsXmlFmt = "<Part><PartNumber>%s</PartNumber><ETag>%s</ETag></Part>"

	partsXml = ""
	
	for (k,v) in self.parts.items():
	    partsXml+=partsXmlFmt%(k,v)
	
	reqXml = "<CompleteMultipartUpload>"+partsXml+"</CompleteMultipartUpload>"
	
	headers = {"Content-Length":str(len(reqXml)),
           "x-rock-meta-time":str(time()),
           "x-rock-meta-coder":"terry",
           "Authorization":"ROCK0 %s:uselesschars" % (self.owner),
           "Date": asctime()}	
	    
        try:
	    curUrl = self.getURI()+"?uploadId="+uploadId 
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "POST", url = curUrl, body = reqXml, headers = Utils.signHeader(headers,"POST"))

            response = conn.getresponse()

            if response.status == 200:
                rsp_data = response.read()
                logging.info(" ".join(["POST", str(self), "OK, result:"]))
                logging.info(rsp_data)
                conn.close()

		print rsp_data
                return True		
            else:
                rsp_data = response.read()
                logging.error(" ".join(["POST", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["POST", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return False	    
    
    def drop(self, url, version = 0, timeout = 60 * 1000 * 5):
        headers = {"Authorization":"ROCK0 %s:uselesschars" % (self.owner),
                   "Date": asctime(),
                   "Content-Length":0}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "DELETE", url = self.getURI(), headers = Utils.signHeader(headers,"DELETE"))
            response = conn.getresponse()

            if response.status == 200:
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
                   "Content-Length":0}
        conn = None
        try:
            conn = HTTPConnection(url, timeout = timeout)
            conn.request(method = "DELETE", url = self.getURI(), headers = Utils.signHeader(headers,"DELETE"))

            response = conn.getresponse()

            if response.status == 200:
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
                logging.error(" ".join(["GET", str(self), ", reason:%s" % (response.status), response.reason, str(rsp_data)]))
        except Exception, e:
            logging.error(" ".join(["GET", str(self), "Failed, exception:", str(e)]))
    	if conn:
    		conn.close()
        return None

from pebble import testPebbleNew    
from pebble import DataPool
from pebble import Pebble
from pebble import testBucketBase
if __name__=="__main__":
    url = "10.24.1.10:8080"
    owner = "yuanfeng.zhang@gmail.com"
    pebbleId = "/pebble_0001/pebble_XXXX_00000001"
    testBucketBase("pebble_0001", url)
    pebble = MultiPartPebble(pebbleId)
    pebble.initMultiPart(url)
    print 
    print 
    pebble.putPart(url,DataPool.getData(13),1 )
    print 
    print     
    pebble.putPart(url,DataPool.getData(26),2)
    print 
    print 
    pebble.listMultiPars(url)
    print 
    print 
    pebble.listParts(url)
    print 
    print 
    pebble.completeMultiPart(url)
    print 
    print 
    pebble.listMultiPars(url)
    print 
    print 
    pebble.listParts(url)    
    print 
    print 
    p = Pebble(pebbleId)
    print "*"*40
    print p.head(url)
    print "*"*40
    print p.get(url)
    #testPebbleNew("/pebble_0001/pebble_XXXX_00000001", url)
    #data = pebble.get(url, 0, True)
from httplib import *
import os
from random import random
from random import shuffle
import logging
from time import time
from time import asctime
import socket
import httplib
import hashlib
from statCalc import StatClient
from threading import Thread


httplib.HTTPConnection.debuglevel=0

def postData(host,url,data):
    assert(data)
    headers = {"Content-Type":"application/octet-stream",
       "Content-Length":str(len(data)),
       "x-rock-meta-time":str(time()),
       "x-rock-meta-coder":"terry",
       "Date": asctime()}
    conn = None
    try:
        conn = HTTPConnection(host)
        conn.request(method = "PUT", url = url, body = data, headers = headers)
        #print self.getURI()
        response = conn.getresponse()
    
        if response.status == 200:
            rsp_data = response.read()
            logging.info(" ".join(["PUT", str(len(data)), " bytes OK, result:"]))
            logging.info(rsp_data)
            conn.close()
            return True
        else:
            rsp_data = response.read()
            logging.error(" ".join(["PUT", str(len(data)), " bytes , reason:%s" % (response.status), response.reason, str(rsp_data)]))
    except Exception, e:
        logging.error(" ".join(["PUT", str(len(data)), " bytes Failed, exception:", str(e)]))
    if conn:
        conn.close()
        return False

class SendThread(Thread):
    def __init__(self,num,data,threadId):
        Thread.__init__(self)
        self.__num = num
        self.__data = data
        self.__threadId = threadId
        
    def run(self):
        for i in range(self.__num):
            postData("localhost","/rockstor/",self.__data)
        print "thread #",self.__threadId,"finished!"
    
if __name__=="__main__":
    logging.basicConfig(level = logging.ERROR,
                        format = '%(asctime)s %(levelname)s %(message)s',
                        filename = None,
                        filemode = 'w')  
    data = "0123456789abcdef"*(1024/16)
    ths = [] 
    thNum = 100
    sendTimes = 1
    for i in range(thNum):
        ths.append(SendThread(sendTimes,data*1024,i))
    for th in ths:
        th.start()
    for th in ths:
        th.join()
    print "test finished!"
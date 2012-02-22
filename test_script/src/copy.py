import os
import sys
from multiprocessing import freeze_support
from multiprocessing.managers import BaseManager, BaseProxy
import shlex, subprocess
from threading import Thread
from threading import RLock

stdoutLock = RLock()

def run(cmd):
	global stdoutLock
        try:
                p = subprocess.Popen(cmd, shell = True, stderr = subprocess.PIPE, stdout = subprocess.PIPE)
                ret = p.communicate()[0]
		if not ret:
			ret = "OK"
        except Exception, e:
                ret = "exception: %s" % (str(e),)
	try:
		stdoutLock.acquire()
		print "exec '", cmd,"', result:",ret
	finally:
		stdoutLock.release()
        return ret

def copy(src,dst,dstIp=None, to=True):
        #if not os.path.exists(os.path.dirname(src)):
        #       print src,"is not exitsts!"
        #       return
        if dstIp:
		if to:
			cmd = "scp -r %s %s:%s"%(src,dstIp,dst)
		else:
			cmd = "scp -r %s:%s %s"%(dstIp,src,dst)
        else:
                cmd = "cp -rf %s %s"%(src,dst)
        run(cmd)

class CpThread(Thread):
	def __init__(self,src,dst,ip,to=True):
		Thread.__init__(self)
		self.__src = src
		self.__dst = dst
		self.__ip = ip
		self.__to = to
	
	def run(self):
		copy(self.__src,self.__dst,self.__ip,self.__to)
			
	
def batchCopy(src,dst,nodesFile,to=True):
        threads = []
        with open(nodesFile,"rb") as fp:
                for line in fp.readlines():
                        line = line.strip()
                        if (not line) or line.startswith("#"):
                                continue
                        threads.append(CpThread(src,dst,line,to))

        for th in threads:
                th.start()
	
	for th in threads:
		th.join()


def usage():
        print "python copy.py src dst [-h ip|-f nodelist.txt] [-from]"
        sys.exit(0)


def main_func():
	argc = len(sys.argv)
        if argc!=3 and argc!=5 and argc!=6:
                usage()

        single = False

        if len(sys.argv)==3:
                copy(src,dst)
                return

        if sys.argv[3] not in ["-h","-f"]:
                usage()

        single = (sys.argv[3]=="-h")
        if single:
                copy(sys.argv[1],sys.argv[2],sys.argv[4],argc!=6)
        else:
                batchCopy(sys.argv[1],sys.argv[2],sys.argv[4], argc!=6)

if __name__=="__main__":
        main_func()



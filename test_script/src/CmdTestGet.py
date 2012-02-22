'''
Created on 2010-11-6

@author: terry
'''
from multiprocessing import freeze_support, Pool
from multiprocessing.managers import BaseManager, BaseProxy
import shlex, subprocess
import os
import sys
from RPCRunner import Shell, ShellManager
from time import time
##

class Node:
    def __init__(self, address, port = 5000, authKey = "terry.xu"):
        from RPCRunner import Shell, ShellManager
        ShellManager.register('Shell', Shell)
        self.manager = ShellManager(address = (address, port), authkey = authKey)
        self.manager.connect()
        self.sh = self.manager.Shell()
        self.address = address
        self.port = port
        self.authKey = authKey
        print self, "init ok"

    def close(self):
        return "%s closed" % (self,)

    def __str__(self):
        return self.address

    def run(self, cmd):
        return os.linesep.join(["%s Execute: %s" % (self, cmd),
                                "-" * 20,
                                self.sh.run(cmd),
                                "-" * 40,
                                ''])

    def kill(self, pid, all = False):
        return os.linesep.join(["%s Kill %s %s:" % (self, all, pid),
                                "-" * 20,
                                self.sh.kill(pid, all),
                                "-" * 40,
                                ''])

def run(address, cmd):
    node = Node(address)
    return node.run(cmd)

def kill(address, cmd):
    node = Node(address)
    return node.kill(cmd)

def killall(address, cmd):
    node = Node(address)
    return node.kill(cmd, True)

def close(address, cmd):
    node = Node(address)
    return node.close()


pool = None

def run_pool(nodes, func, cmd):
    global pool
    resultSet = [x.get() for x in [pool.apply_async(func, (node, cmd)) for node in nodes]]
    for result in resultSet:
        print result

#cmdpairs [(exec_node, cmd),(exec_node, cmd)]
#used to assign different task to different nodes
def run_batch(func, cmdpairs):
    global pool
    resultSet = [x.get() for x in [pool.apply_async(func, node_cmd) for node_cmd in cmdpairs]]
    for result in resultSet:
        print result

##
def testGetPebble():
    global pool
    ShellManager.register('Shell', Shell)

    help_str = "./python CmdClient.py nodeNum servletRoot pebblepPrefix pebbleSize procNum pebbleNum"

    nodesIP = [#"10.24.1.41",
              "10.24.1.42",
              "10.24.1.43",
              "10.24.1.44",
              "10.24.1.45",
              "10.24.1.46",
              "10.24.1.47",
              "10.24.1.48",
              #"10.24.1.49"
              ]

    servletIP = [#"10.24.1.41",
              "10.24.1.42",
              "10.24.1.43",
              "10.24.1.44",
              "10.24.1.45",
              "10.24.1.46",
              "10.24.1.47",
              "10.24.1.48",
              #"10.24.1.49"
              ]

    if len(sys.argv) != 7:
        print help_str
        print
        return

    (nodeNum, servletRoot, pebbleId0, pebbleSize, pebbleProcess, pebbleNum) = sys.argv[1:]

    nodeNum = int(nodeNum)

    if nodeNum > len(nodesIP):
        nodeNum = len(nodesIP)

    pebbleSize = int(pebbleSize)
    pebbleProcess = int(pebbleProcess)
    pebbleNum = int(pebbleNum)

    for k in (nodeNum, servletRoot, pebbleId0, pebbleSize, pebbleProcess, pebbleNum) :
        if not k:
            print help_str
            print
            return

    #pebbleId0 = "pebble70"

    #servletRoot = "/rockstor"
    servletPort = 8080
    #pebbleSize = 40
    #pebbleProcess = 4
    #pebbleNum = 30000

    proc_num = nodeNum

    nodesIP = nodesIP[0:nodeNum]


    #nodes = [ Node(address) for address in nodesIP]

    nodes = nodesIP

    master_svr = "10.24.1.41"

    putCmd = "nohup ./python rockstor/test.py %s BatchGet %%s:%s %s_%%s %s %s 1 1 0 &"
    syncFileCmd = "scp result/get/fileMerge/* %s:/home/schubert/terry/test/files/get/" % (master_svr,)
    mergeResultCmd = "./python rockstor/mergeFile.py result ./files/get/ ./merge/get/"
    cleanCmd = "rm -rf ./files/get/* merge/get/*"

    curPutCmd = putCmd % (servletRoot, servletPort, pebbleId0, pebbleProcess, pebbleNum)

    servletNum = len(servletIP)
    pebbleputs = [(node, curPutCmd % (servletIP[i % servletNum], i + 1)) for i, node in enumerate(nodesIP)]
    fileCollects = [(node, syncFileCmd) for node in nodesIP]
    cleans = [(node, cleanCmd) for node in (master_svr,)]
    merges = [(node, mergeResultCmd) for node in (master_svr,)]

    for s in pebbleputs:
        print s

    print
    for s in fileCollects:
        print s

    print
    for s in cleans:
        print s

    print
    for s in merges:
        print s

    pool = Pool(processes = proc_num)

    run_batch(run, cleans)
    run_batch(run, pebbleputs)
    run_batch(run, fileCollects)
    run_batch(run, merges)
    return

if __name__ == '__main__':
    freeze_support()
    testGetPebble()


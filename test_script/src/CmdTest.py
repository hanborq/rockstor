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
from time import time,ctime,sleep
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
    start_ts = time()
    start_ts_str = ctime(start_ts)
    print sys.argv,"start at:",start_ts_str
    resultSet = [x.get() for x in [pool.apply_async(func, node_cmd) for node_cmd in cmdpairs]]
    for result in resultSet:
        print result
    stop_ts = time()
    stop_ts_str = ctime(stop_ts)
    waste_time = stop_ts - start_ts
    print sys.argv,"stop at:",stop_ts_str
    print "cmd %s, from %s to %s, waste time:  %.4f sec"%(start_ts_str,stop_ts_str,waste_time)
    return [start_ts_str,stop_ts_str,waste_time]
    
##
def testPutPebble():
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
    servletPort = 48080
    #pebbleSize = 40
    #pebbleProcess = 4
    #pebbleNum = 30000

    proc_num = nodeNum

    nodesIP = nodesIP[0:nodeNum]


    #nodes = [ Node(address) for address in nodesIP]

    nodes = nodesIP

    putCmd = "nohup ./python rockstor/test.py %s BatchFixPut %%s:%s %s_%%s  %s %s %s 1 &"

    curPutCmd = putCmd % (servletRoot, servletPort, pebbleId0, pebbleSize, pebbleProcess, pebbleNum)

    servletNum = len(servletIP)
    pebbleputs = [(node, curPutCmd % (servletIP[i % servletNum], i + 1)) for i, node in enumerate(nodesIP)]

    for s in pebbleputs:
        print s
    print

    pool = Pool(processes = proc_num)

    [start_ts_str,stop_ts_str,waste_time] = run_batch(run, pebbleputs)
    
    total_num = pebbleNum*pebbleProcess*nodeNum
    total_size = total_num*pebbleSize
    
    print "########################################"
    print "op        type:","PutPebble"
    print "node    Number:",nodeNum
    print "servlet Number:",servletNum
    print "pebble    Size:",pebbleSize,"KB"
    print "process Number:",pebbleProcess,"/node"
    print "Result:(if all succeed)"
    print "start     time:",start_ts_str
    print "stop      time:",stop_ts_str
    print "waste     time:",waste_time
    print "throughput    :",total_num/waste_time,"ops/sec"
    print "io        rate:",total_size/waste_time,"KB/sec"
    print "########################################"   

##
def testGetPebble():
    global pool
    ShellManager.register('Shell', Shell)

    help_str = "./python CmdClient.py nodeNum servletRoot pebblepPrefix pebbleSize procNum pebbleNum"

    nodesIP = ["10.24.1.41",
              #"10.24.1.42",
              #"10.24.1.43",
              #"10.24.1.44",
              #"10.24.1.45",
              #"10.24.1.46",
              #"10.24.1.47",
              #"10.24.1.48",
              #"10.24.1.49"
              ]

    servletIP = ["10.24.1.41",
              #"10.24.1.42",
              #"10.24.1.43",
              #"10.24.1.44",
              #"10.24.1.45",
              #"10.24.1.46",
              #"10.24.1.47",
              #"10.24.1.48",
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
    servletPort = 48080
    #pebbleSize = 40
    #pebbleProcess = 4
    #pebbleNum = 30000

    proc_num = nodeNum

    nodesIP = nodesIP[0:nodeNum]


    #nodes = [ Node(address) for address in nodesIP]

    nodes = nodesIP

    putCmd = "nohup ./python rockstor/test.py %s BatchGet %%s:%s %s_%%s %s %s 1 &"

    curPutCmd = putCmd % (servletRoot, servletPort, pebbleId0, pebbleProcess, pebbleNum)

    servletNum = len(servletIP)
    pebbleputs = [(node, curPutCmd % (servletIP[i % servletNum], i + 1)) for i, node in enumerate(nodesIP)]

    for s in pebbleputs:
        print s
    print

    pool = Pool(processes = proc_num)

    [start_ts_str,stop_ts_str,waste_time] = run_batch(run, pebbleputs)
    
    total_num = pebbleNum*pebbleProcess*nodeNum
    total_size = total_num*pebbleSize
    
    print "########################################"
    print "op        type:","GetPebble"
    print "node    Number:",nodeNum
    print "servlet Number:",servletNum
    print "pebble    Size:",pebbleSize,"KB"
    print "process Number:",pebbleProcess,"/node"
    print "Result:(if all succeed)"
    print "start     time:",start_ts_str
    print "stop      time:",stop_ts_str
    print "waste     time:",waste_time
    print "throughput    :",total_num/waste_time,"ops/sec"
    print "io        rate:",total_size/waste_time,"KB/sec"
    print "########################################"   
    return


def test():
    global pool
    ShellManager.register('Shell', Shell)

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

    nodeNum = len(nodesIP)
    proc_num = nodeNum
    nodesIP = nodesIP[0:nodeNum]

    nodes = nodesIP

    pool = Pool(processes = proc_num)

    while True:
        cmd = raw_input("Command: ")
        if not cmd:
            print
            continue
        print
        print cmd
        print
        while True:
            try:
                if cmd.startswith("quit"):
                    run_pool(nodes, close, cmd)
                    print "good bye"
                    pool.close()
                    exit(0)
                    return
                if cmd.startswith("kill"):
                    cmd = cmd.split()[1:]
                    if not cmd:
                        break

                    run_pool(nodes, kill, " ".join(cmd))
                    break
                if cmd.startswith("kall"):
                    cmd = cmd.split()[1:]
                    if not cmd:
                        break

                    run_pool(nodes, killall, " ".join(cmd))
                    run_pool(nodes, close, cmd)
                    pool.close()
                    exit(0)
                run_pool(nodes, run, cmd)
            except Exception, e:
                print e
                print
            break
        print "-" * 30



if __name__ == '__main__':
    freeze_support()
    argc = len(sys.argv)
    help_str = """
                ./python CmdTest.py put nodeNum servletRoot pebblepPrefix pebbleSize procNum pebbleNum
                ./python CmdTest.py get nodeNum servletRoot pebblepPrefix pebbleSize procNum pebbleNum
                ./python CmdTest.py pg nodeNum servletRoot pebblepPrefix pebbleSize procNum pebbleNum       
               """
    if argc != 8:
        print help_str
        #run_batch(None,[])
        exit(0)
    op = sys.argv[1]
    sys.argv = sys.argv[1:]

    if op == 'put':
        testPutPebble()
        exit(0)
    if op == 'get':
        testGetPebble()
        exit(0)
    if op == 'pg':
        testPutPebble()
        sleep(30)
        testGetPebble()
        exit(0)
    print help_str
    exit(0)

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
from copy import batchCopy
##
if "linux" in sys.platform:
	import readline
	histfile = os.path.join(os.environ["HOME"], ".pyhist")
	try:
		readline.read_history_file(histfile)
	except IOError:
		pass
	import atexit
	atexit.register(readline.write_history_file, histfile)
	del histfile
    

class Node:
    serv_port = 5000
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

def run(address, port, cmd):
    node = Node(address, port)
    return node.run(cmd)

def kill(address, port, cmd):
    node = Node(address, port)
    return node.kill(cmd)

def killall(address, port, cmd):
    node = Node(address, port)
    return node.kill(cmd, True)

def close(address, port, cmd):
    node = Node(address, port)
    return node.close()

pool = None

def run_pool(nodes, func, cmd):
    global pool
    resultSet = [x.get() for x in [pool.apply_async(func, (node, Node.serv_port, cmd)) for node in nodes]]
    for result in resultSet:
        print result

#cmdpairs [(exec_node, port, cmd),(exec_node, port, cmd)]
#used to assign different task to different nodes
def run_batch(func, cmdpairs):
    global pool
    resultSet = [x.get() for x in [pool.apply_async(func, node_cmd) for node_cmd in cmdpairs]]
    for result in resultSet:
        print result

def loadnodes(fname):
    nodes = []
    with open(fname, "rb") as fp:
        lines = fp.readlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue
            nodes.append(line)

    print "---- Node Start ---"
    for i, node in enumerate(nodes):
        print i + 1, node
    print "--- Node End ---"
    print "-- total %s nodes--" % (len(nodes))
    print
    return nodes

def test():
    global pool

    nodelistFile = sys.argv[1]

    if not os.path.exists(nodelistFile) or not os.path.isfile(nodelistFile):
        print "node list file", nodelistFile, "not exist or not a file"
        return
    nodes = loadnodes(nodelistFile)

    proc_num = len(nodes)

    if not proc_num:
        print "node list file", nodelistFile, " has no valid nodes"
        return

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
                if cmd.startswith("reload"):
                    cmd = [x for x in cmd.split(" ")[1:] if x]
                    if not cmd:
                        nodefile = nodelistFile
                    else:
                        nodefile = cmd[0]

                    if not os.path.exists(nodefile) or not os.path.isfile(nodefile):
                        print "node list file", nodefile, "not exist or not a file"
                        print
                        break

                    nodes = loadnodes(nodefile)
                    break
	        if cmd.startswith("cpTo"):
                    cmd = [x for x in cmd.split(" ")[1:] if x]
                    if (not cmd) or ((not (len(cmd)==2)) and (not (len(cmd)==3))):
                        print "cp src dst [nodes.txt]"
                    else:
			curFile = nodelistFile
			if len(cmd)==3:
				curFile = cmd[2]
			batchCopy(cmd[0],cmd[1],curFile)
		    break
	    
	        if cmd.startswith("cpFrom"):
                    cmd = [x for x in cmd.split(" ")[1:] if x]
                    if (not cmd) or ((not (len(cmd)==2)) and (not (len(cmd)==3))):
                        print "cp src dst [nodes.txt]"
                    else:
			curFile = nodelistFile
			if len(cmd)==3:
				curFile = cmd[2]
			batchCopy(cmd[0],cmd[1],curFile,False)
		    break	    
	    
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

    help_str = "python CmdClient nodelist.txt [port]"
    if len(sys.argv) > 3:
        print help_str
        exit(0)
    if len(sys.argv) == 3:
        try:
            Node.serv_port = int(sys.argv[2])
        except:
            print help_str
            exit(0)
    test()



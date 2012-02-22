'''
Created on 2010-11-6

@author: terry
'''
from multiprocessing import freeze_support
from multiprocessing.managers import BaseManager, BaseProxy
import shlex, subprocess
import os
import sys
from RPCRunner import Shell, ShellManager
##

##
def showusage():
    print """
            python CmdServer.py [port]
              default port 5000
          """

def test():
    ShellManager.register("os", os)
    ShellManager.register('Shell', Shell)

    argc = len(sys.argv)

    port = 5000

    if argc > 2:
        showusage()
        return

    if argc == 2:
        try:
            port = int(sys.argv[1])
        except:
            showusage()
            return

    manager = ShellManager(address = ('', port), authkey = "terry.xu")
    s = manager.get_server()
    print "cmd server started, listen port:", port
    s.serve_forever()

if __name__ == '__main__':
    freeze_support()
    test()

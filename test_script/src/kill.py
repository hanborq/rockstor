import os
import sys
from multiprocessing import freeze_support
from multiprocessing.managers import BaseManager, BaseProxy
import shlex, subprocess


def run(cmd):
    print "exec", cmd
    try:
        p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        ret = p.communicate()[0]
    except Exception, e:
        print e
        ret = "Exception: exec %s, exception: %s" % (cmd, str(e),)
    print ret
    print "-" * 20
    print
    return ret

def kill(cmd, all=False):
    ps_cmd = "ps -ef | grep %s" % (cmd)
    lines = run(ps_cmd)
    if not lines:
        return "No Such process"
    if lines.startswith("Exception:"):
        return "Exception: exec kill %s, exception: %s" % (cmd, lines)

    kill_cmd = "kill -9 %s"
    pids = []
    lines = [line for line in lines.split(os.linesep) if line]

    #lines.pop()

    if not lines:
        return "No Such process"

    pids = []
    for line in lines:
        line = line.strip()
        if not line: continue
        line = line.split()
        if len(line) < 6: continue
        line = [line[0], line[1].strip(), line[2], " ".join(line[7:])]
        try:
            line[1] = int(line[1])
            line[2] = int(line[2])
        except:
            continue
        pids.append(line)
        
    toKills = []
    for pid in pids:
        if pid[3].find(cmd) != -1:
            if pid[3].find("rockstor/kill.py") == -1:
                if all or pid[3].find("CmdServer") == -1:
                    toKills.append(pid)

    print "prepare to kill"
    toKills = sorted(toKills, lambda x, y:x[2] - y[2])
    for pid in toKills:
        print pid
    print
    rets = []

    for pid in toKills:
        rets.append(run(kill_cmd % (pid[1],)))
    ret_lines = os.linesep.join([str(i) for i in rets])
    print "Result: "
    print ret_lines
    print
    return os.linesep.join([str(pid) for pid in toKills])

if __name__ == "__main__":
    if len(sys.argv) == 2:
        kill(sys.argv[1])
    elif len(sys.argv) == 3:
        kill(sys.argv[1], sys.argv[2] == "True")
    

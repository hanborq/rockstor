import os
import sys
import threading
def getFiles(dirName,spec):
    if not( os.path.exists(dirName) and os.path.isdir(dirName)):
        print "input dir",dirName,"is invalid"
        return [None,None]

    thFiles = []
    delayFiles = []

    thSuffix = "th.txt"
    delaySuffix = "delay.txt"
    
    files = os.listdir(dirName)
    if spec:
        for f in files:
            fpath = os.path.join(dirName,f)
            if f.endswith(thSuffix) and f.find(spec)>0:
                thFiles.append(fpath)
            elif f.endswith(delaySuffix) and f.find(spec)>0:
                delayFiles.append(fpath)
            else:
                print "invalid file",fpath       
    else:    
        for f in files:
            fpath = os.path.join(dirName,f)
            if f.endswith(thSuffix):
                thFiles.append(fpath)
            elif f.endswith(delaySuffix):
                delayFiles.append(fpath)
            else:
                print "invalid file",fpath
    return [thFiles,delayFiles]

# key, timestamp; value=[num, minDelay, maxDelay]
class mergeThroughput(threading.Thread):    
    def __init__(self,files,outName, dojob = False):
        threading.Thread.__init__(self)
        self.files =files
        self.outName = outName
        self.dojob = dojob
        print "init merge throughput thread" 
        
    def run(self):
        if not self.dojob or not self.files or not self.outName: return
        files = self.files
        outName = self.outName  
        
        t = {}
        total_files = len(files)
        for i,f in enumerate(files):
            with open(f,"rb") as fin:
                for j,line in enumerate(fin.readlines()):
                    line = line.strip()
                    items = line.split(" ")
                    if len(items)< 7:
                        print f,"invalid line:",j,line
                        continue
                    ts = int(items[0])
                    num = int(items[1])
                    minDelay = int(items[2])
                    maxDelay = int(items[3])
                    reDelay = int(items[4])
                    
                    if t.has_key(ts):
                        pre = t[ts]
                        pre[0] += num
                        if pre[1]>minDelay:
                            pre[1] = minDelay
                        if pre[2]<maxDelay:
                            pre[2] = maxDelay
                        pre[3]+= reDelay
                    else:
                        t[ts] = [num,minDelay,maxDelay,reDelay]
                print i,"/",total_files,"merge file",f,"OK"
        format_str = "%s %s %s %s %.3f %s %s %s %.3f %.3f"+os.linesep
        with open(outName,"wb+") as fout:
            k = t.items()
            k = sorted(k,lambda x,y:int(x[0]-y[0]))
            interval = 120*1000
            start_ts = k[0][0] - interval
            total_num = 0
            minD = 900000000000
            maxD = 0
            for (ts,[num,minDelay,maxDelay,reDelay]) in k:
                delta = ts - start_ts
                total_num += num
                if minD>minDelay:
                    minD = minDelay
                if maxD <maxDelay:
                    maxD = maxDelay
                    
                fout.write(format_str%(ts,num,minDelay,maxDelay, float(reDelay)/num,total_num,minD,maxD, (float(num)*1000)/interval,(float(total_num)*1000)/delta))
        print "merge th ok!"
        print "-------------------"
        print   


class mergeDelay(threading.Thread):
    def __init__(self,files,outName, dojob = False):
        threading.Thread.__init__(self)
        self.files =files
        self.outName = outName
        self.dojob = dojob
        print "init merge delay thread"
    
    def run(self):
        if not self.dojob or not self.files or not self.outName: return
        files = self.files
        outName = self.outName       
        t = {}
        total_files = len(files)
        countAll = 0
        for i,f in enumerate(files):
            with open(f,"rb") as fin:
                for j,line in enumerate(fin.readlines()):
                    line = line.strip()
                    items = line.split(" ")
                    if not len(items)==2:
                        print f,"invalid line:",j,line
                        continue
                    delay = int(items[0])
                    count = int(items[1])
                    countAll += count
                    if t.has_key(delay):
                        t[delay] += count
                    else:
                        t[delay] = count
                    
                    #print i,"/",total_files,"merge file",f,"OK"
        format_str = "%s %s %d"+os.linesep
        with open(outName,"wb+") as fout:
            k = t.items()
            k = sorted(k,lambda x,y:x[0]-y[0])
            for (delay,count) in k:
                fout.write(format_str%(delay,count, (count*1000)/countAll))
        print "merge delay ok!"
        print "-------------------"
        print    


def merge(dirName,out_prefix,spec=None):
    [thFiles,delayFiles] = getFiles(dirName,spec)
    th = mergeThroughput(thFiles,out_prefix+"_th.txt",True)
    delay = mergeDelay(delayFiles,out_prefix+"_delay.txt",True)   
    #th.start()
    #delay.start()
    
    #th.join()
    #delay.join()
    return [th, delay]
    
def test():
    [thFiles,delayFiles] = getFiles("./64k/get/size")
    l = len("fc_p4k_1_0063")
    
    for f in thFiles:
        print "\""+os.path.basename(f)[0:l]+"/"+os.path.basename(f)[0:l]+"_\","

        
    print len(thFiles)
    print thFiles[0]
    th = mergeThroughput(thFiles,"./64k/through_get.txt",True)
    
    print len(delayFiles)
    print delayFiles[0]
    delay = mergeDelay(delayFiles,"./64k/delay_get.txt",True)
    
    #th.start()
    #delay.start()
    
    #th.join()
    #delay.join()
    return [th, delay]

def main_func():
    ths = []
    usage="python mergeResult.py srcDir dstDir"
    
    if len(sys.argv)!=3:
        print usage
        return
    
    (srcDir,dstDir) = sys.argv[1:]
    
    if not os.path.exists(srcDir):
        print srcDir,"not existed"
        print usage    
        return
    
    if not os.path.exists(dstDir):
        os.makedirs(dstDir)
    
    ths.extend(merge(srcDir,os.path.join(os.path.abspath(dstDir),"final")))
    
    #ths.extend(merge("./result_bak/fixput/size_16k","./16k_put"))
    #ths.extend(merge("./result_bak/fixput/size_64k","./64k_put"))    

    #ths.extend(merge("./result_bak/get/size_8k","./8k_get"))
    #ths.extend(merge("./result_bak/get/size_16k","./16k_get"))
    #ths.extend(merge("./result_bak/get/size_64k","./64k_get"))
    
    for th in ths:
        if th:
            th.start()
        
    for th in ths:
        if th:
            th.join()
    
if __name__=="__main__":
    main_func()
    
from pebble import *

url = "terry-pc-serv:8080"
owner = "chengjie.xu@gmail.com"
bucketID = "k64"
num =  24576
dataSize = (64<<10)
data = DataPool.getData(dataSize)

def initBucket():
    global bucketID,url
    Bucket.hdfsRoot = "/rockstor"
    bucket = Bucket(bucketID)
    bucket.put(url)
    bucket.get(url)
    bucket.getACL(url) 

def putPebble():
    global bucketID,num,dataSize,url
    pebbleIDFormatStr = "/"+bucketID+"/p_%05d"
    for i in range(1,num+1):
        pebble = Pebble(pebbleIDFormatStr%(i))
        pebble.put(url, data)    
        if i%100 ==0:
            print "put",i,"/",num
  
def dropPebble():
    global bucketID,num,dataSize,url
    pebbleIDFormatStr = "/"+bucketID+"/p_%05d"
    for i in range(11,num+1): # left ten pebble
        pebble = Pebble(pebbleIDFormatStr%(i))
        pebble.drop(url)    
        if i%100 ==0:
            print "drop",i,"/",num
            
if __name__=="__main__":
    initBucket()
    putPebble()
    dropPebble()


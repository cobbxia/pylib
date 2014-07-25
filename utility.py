#/usr/bin/env python
import datetime
import threading
import urllib
import os
import sys
import time
import subprocess
import re
import logging

def printTime():
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print(nowTime)

def now_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


def isNum(str):
    if re.search(r'^\s*[+-]?\d+\.?\d*\s*$',str):
        return True
    else:
        return False

def do_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return p.returncode, out, err

def lighterFile(fa,fb):
    itemA=fa.split('/')[-1]
    dirA=int(fa.split('/')[-2])
    itemB=fb.split('/')[-1]
    dirB=int(fa.split('/')[-2])
    secondA=int(itemA.split('_')[0])+1024*dirA
    secondB=int(itemB.split('_')[0])+1024*dirB
    nanoA=int(itemA.split('_')[1])
    nanoB=int(itemB.split('_')[1])
    if secondA < secondB:
        return 1
    elif secondA==secondB:
        if nanoA < nanoB:
            return 1
    else:
        return 0

def bubbleSortSQL(pathList,lighter):
    maxLen=len(pathList)
    i=0
    while i<maxLen:
        j=maxLen-1
        while j>i:
#     print(pathList[j])
            if lighter(pathList[j],pathList[j-1]):
                temp=pathList[j]
                pathList[j]=pathList[j-1]
                pathList[j-1]=temp
            j=j-1
        i=i+1
    return pathList

def execSQLFile(HQLfile,logfile,hive="hive"):
    cmd="%s -f %s >> %s 2>&1" % (hive,HQLfile,logfile)
    print(cmd)
    r=do_cmd(cmd)
    return r

def exeHQL(HQL,hiveDriver="hive"):
    cmd='''export IDE_HIVE_USER_REGION=mingchao.xiamc;export porjectName=mingchao.xiamc;%s -e "%s" ''' % (hiveDriver,HQL)
    print(cmd)
    ret=1
    rList=[]
    r=do_cmd(cmd)
    if not re.search(r'\s*[oO][kK]\s*',r[2].decode('utf-8')):
        print("exec %s error!" % HQL)
        ret=1
    else:
        print(r[1].decode('utf-8'))
        print(r[2].decode('utf-8'))
        ret=0
    for i in range(len(r)): 
        rList.append(r[i])
    rList.append(ret)
    return rList

def initlog(logfile):
    logger = logging.getLogger()
    hdlr = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.NOTSET)
    return logger


def createList(fileName):
    fin=open(fileName,"r")
    BList=[]
    for line in fin:
        line=line.strip("\n")
        if line == "":
            continue
        BList.append(line)
    fin.close()
    return BList

def createDict(fileName):
    fin=open(fileName,"r")
    BDict={}
    for line in fin:
        line=line.strip("\n")
        if line == "":
            continue
        BDict[line]=0
    fin.close()
    return BDict

def getCurRunPosInfo():
    try:
        raise Exception
    except:
        exc_info = sys.exc_info()
        traceObj = exc_info[2]
        frameObj = traceObj.tb_frame
        Upframe = frameObj.f_back
        return (Upframe.f_code.co_filename, Upframe.f_code.co_name, Upframe.f_lineno)

class getCheck(threading.Thread):
    def __init__(self,threadSum,index,taskList,resDir):
        threading.Thread.__init__(self)
        self.__threadSum=threadSum
        self.__taskList=taskList
        self.__index=index
        self.size=len(self.__taskList)*((self.__index+1.0)/threadSum)
        self.totalSize=len(self.__taskList)
        self.beginNum=int(len(self.__taskList)*((self.__index+0.0)/threadSum))
        self.failNum=0
        self.succNum=0
        self.failPathList=[]
        self.passPathList=[]
        do_cmd("mkdir -p %s" % resDir)
        self.filename=resDir+"/thread_"+str(self.__index)
        self.failList=[]
        do_cmd("mkdir -p ./logging")
        print("[%s:%s] threadSum:%s threadIndex:%s Size:%s start:%s" % (getCurRunPosInfo()[1],getCurRunPosInfo()[2],self.__threadSum,self.__index,self.size,self.beginNum))

    def setProcess(self,func):
        self.processFunc=func

    def run(self):
        logfile="./logging/test%s.log" % self.__index
        #user="hivetest%s" % ((self.__index)%(self.__threadSum)+1)
        user="hivetest1"
        i=self.beginNum
        while i<self.size and i<self.totalSize :
            print("process task:%s" % self.__taskList[i])
            if self.processFunc:
                #task="set hive.metastore.warehouse.dir=/group/public/%s;set mapred.working.dir=/group/public/%s;set hadoop.job.ugi=%s,#taobao1234;" % (user,user,user)
                task="set hive.metastore.warehouse.dir=/group/public/%s;set mapred.working.dir=/group/public/%s;set hadoop.job.ugi=%s,#taobao1234;" % (user,user,user)
                task=task+self.__taskList[i]
                r=self.processFunc(task)
                if r != 0:
                    self.failPathList.append(task)
                    self.failNum=self.failNum+1
                else:
                    self.passPathList.append(task)
                    self.succNum=self.succNum+1
            else:
                 print("process function not defined")
                 return -1
            i=i+1

    def getNum(self):
        return (self.succNum,self.failNum)
    def getFailPath(self):
        return self.failPathList
    def getPassPath(self):
        return self.passPathList

def createSQLList(rootPath):
    urlList=[]
    pathList=[]
    for dirName in os.listdir(rootPath):
        dirName=rootPath+"/"+dirName
        for fileName in os.listdir(dirName):
            fullFileName="%s//%s" % (dirName,fileName)
            cmd="/bin/cat %s" % fullFileName
            print(cmd)
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if p.returncode == 0:
                urlList.append(out)
                pathList.append(fullFileName)
    return (pathList,urlList)

def createPathList(rootPath):
    pathList=[]
    for dirName in os.listdir(rootPath):
        dirName=rootPath+"/"+dirName
        pathList.append(dirName)
    return pathList

def doMain(threadSum,taskList,farg):
    urlList=[]
    pathList=[]
    threads=[]
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print("CheckClient Info:[%s] Start Checking  work paralell in %s thread." % (nowTime,threadSum))
    qLine=""
    cnt=1
    cnt=len(taskList)
    print("got total lines:%s" % (cnt))
    if cnt < threadSum:
        threadSum=cnt
    resDir="./res"
    for index in range(threadSum):
        getcheck=getCheck(threadSum,index,taskList,resDir)
        getcheck.setProcess(farg)
        threads.append(getcheck)
    print("------------------------------------------------------------------------------")
    for index in range(threadSum):
        print("create thread:%s" % (index))
        threads[index].start()
    succTotal=0
    failTotal=0
    failList=[]
    pf=open("/tmp/pass.log","a")
    ff=open("/tmp/failure.log","a")
    for index in range(threadSum):
        (succNum,failNum)=threads[index].getNum()
        succTotal=succTotal+succNum
        failTotal=failTotal+failNum

        fialPathList=[]
        fialPathList=threads[index].getFailPath()
        for item in fialPathList:
            ff.write(item)
            ff.write("\n")

        passPathList=[]
        passPathList=threads[index].getPassPath()
        for item in passPathList:
            pf.write(item)
            pf.write("\n")
        print("destroy thread:%s" % (index))
        threads[index].join()
    ff.close()
    pf.close()
    print("------------------------------------------------------------------------------")
    print("succTotal:%s\nfailTotal:%s" % (succTotal,failTotal))
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print("CheckClient Info:[%s] finished Checking  work paralell in %s thread." % (nowTime,threadSum))

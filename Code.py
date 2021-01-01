import pymongo
import time
import threading
from threading import *
client=pymongo.MongoClient('mongodb://127.0.0.1:27017')
mydb=client["newfile"]    
info=mydb.myinfo 
def create(key,value,tout=0):
    a = []
    for i in info.find():
        a.append(i)
    x=len(a)
    if key.isalpha():
        if len(a)<(1024*1024*1024) and value<=(16*1024*1024):
            if tout==0:
                l=[value,tout]
            else:
                l=[value,time.time()+tout]
            if len(key)<=32:
                for j in range(x):
                    if key not in a[j]:
                        if j==x-1:
                            info.insert_one({key:l})                     
                    elif key in a[j]:
                        print("Error : The Key is already Exists!")
                        break
            else:
                print("Error : Key Length is More than 32 Character")
        else:
            print("Error : Memory Limit exceeds!!!") 
    else:
        print("Error : Invalid Key, The Key should be in Alphabet")


def read(key):
    l1=[]
    for i in info.find({},{"_id":0}):
        l1.append(i)
    x=len(l1)
    for i in range(x):
        if key in l1[i]:
            p=l1[i]
            q=list(p.values())
            b=q[0]
            if b[1]!=0:
                if time.time()<b[1]:
                    stri=str(key)+":"+str(b[0])
                    return stri
                else:
                    info.delete_one(l1[i])
                    print("Error : Time to Live of",key,"Has Expired")
                    break
            else:
                stri=str(key)+":"+str(b[0])
                return stri
        else:
            if x-1==i:
                print("Error :",key,"Not Exists")
            else:
                continue
    

def delete(key):
    b=[]
    for i in info.find({},{"_id":0}):
        b.append(i)
    y=len(b)
    for i in range(0,y+1):
        if key in b[i]:
            p=b[i]
            q=list(p.values())
            b=q[0]
            if b[1]!=0:
                if time.time()<b[1]:
                    info.delete_one(p)                    
                    print("Key deleted Successfully")
                    break
                else:
                    info.delete_one(p)
                    print("Error : Time to live of",key,"has expired")
                    break
            else:
                info.delete_one(p)                
                print("Key deleted Successfully")
                break
        else:
            if y-1==i:
                print("Key Not Found")
                break
            else:
                continue

                
if __name__=="__main":    
    
      
    

    t1=threading.Thread(target=create,args=[key,value,tout])               
    t2=threading.Thread(target=read,args=[key])               
    t3=threading.Thread(target=delete,args=[key])               
    t1.start()
    t2.start()
    t3.start()

            




    
    

    

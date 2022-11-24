import sys
import requests
import pandas as pd
import json

def mkdir(url):
    requests.put(url, '{"0":Null}')

def ls(url):
    response = requests.get(url)
    print(*list(response.json().keys()), sep=" ")

def cat(url):
    response = requests.get(url)
    partitions = list(response.json()["partitions"].values()) #get partition info of the file
    
    #display
    for p in partitions:
        r = requests.get(p)
        pretty = json.dumps(r.json(), indent=4)
        print(pretty) #how to display it in a beautiful way

def rm(url):
    response = requests.get(url)
    partitions = list(response.json()["partitions"].values()) #get partition info of the file
    
    #delete the partitions of the file
    for p in partitions:
        requests.delete(p) 

    requests.delete(url)
        
def put(file, url_meta, url_partition , k):
    in_csv = file 
    file_dir = '/'+ in_csv.split('.')[0]
    file = open(file,encoding='utf-8')
    number_lines = sum(1 for row in file)
    partition_size = -(-number_lines // k)
    df_header = list(pd.read_csv(in_csv).columns)
    n = 0
    for i in range(1,number_lines,partition_size):
        n += 1
        to_url = url_partition + file_dir  + '_partition_' + str(n) + '/.json'
        df = pd.read_csv(in_csv,names=df_header,nrows = partition_size,skiprows = i)
        data = df.to_json(orient='records')
        requests.put(to_url,data)
        partition_meta = '{{"{0}" : "{1}"}}'.format("p"+str(n),to_url)
        requests.patch(url_metadata + file_dir +'/partitions.json',str(partition_meta))

    file.close()

def getPartitionLocations(file): #assume file is in the form of url?
    response = requests.get(file)
    partitions = list(response.json()["partitions"].items())
    for i in partitions:
        print(i[0] + ": " + i[1])


def readPartition(file, partition_num):
    response = requests.get(file)
    p = "p"+str(partition_num)
    partition = response.json()["partitions"][p] #return the partition location indicated

    for i in requests.get(partition).json():
        print(i)


if __name__ == "__main__":
    
    url = 'https://dsci551final-52aac-default-rtdb.firebaseio.com' + sys.argv[2] + '/.json'
    
    if sys.argv[1] == "mkdir":
        
        mkdir(url) 

    elif sys.argv[1] == "ls":
        ls(url)

    elif sys.argv[1] == "cat":
        cat(url)
        
    elif sys.argv[1] == "rm":
        rm(url)

    elif sys.argv[1] == "put":
        file = sys.argv[2]
        url_metadata = 'https://dsci551final-52aac-default-rtdb.firebaseio.com/metadata/root/' #need to modify later
        url_partition = 'https://dsci551final-52aac-default-rtdb.firebaseio.com/partition'
        k = int(sys.argv[3])
        put(file,url_metadata,url_partition,k)
        
    elif sys.argv[1] == "getPartitionLocations":
        getPartitionLocations(url)

    elif sys.argv[1] == "readPartition":
        partition_num = sys.argv[3]
        readPartition(url,partition_num)     

    else:
        print("Plase enter a valid command")  


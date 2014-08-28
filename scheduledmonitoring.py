#! /usr/bin/python
import threading
import os
region='Trento'
import datetime

def moveFolder():
    actualSample=datetime.date.fromordinal(datetime.date.today().toordinal()-1)
    actualTxt=actualSample.strftime('%d_%m_%Y')
    #now=datetime.datetime.now()
    #actualTxt=now.strftime('%d_%m_%Y');
    print "creating working-folder.."
    os.system('/usr/bin/hdfs dfs -mkdir /user/hdfs/'+region+'-working')
    print "Moving all dayly files.." 
    os.system('/usr/bin/hdfs dfs -mv /user/hdfs/'+region+'/*'+actualTxt+'*  /user/hdfs/'+region+'-working ')
    #Move the folder

def removeFolder():
    print ("removing working-folder")
    os.system('/usr/bin/hdfs dfs -rm -r  /user/hdfs/'+region+'-working')
    #Remove the folder

def mapredRegion():
    #region mapred
    print "[RE] Remove region folder (if present)"
    os.system('/usr/bin/hdfs dfs -rm -r /user/hdfs/out/region/'+region)
    print "[RE] Start region map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperRegion.py -mapper mapperRegion.py -file /home/xifi-mapReducer/reducerRegion.py -reducer reducerRegion.py -input /user/hdfs/'region'-working/'+region+'-region-*.txt -output /user/hdfs/out/region/'+region);
    print "[RE] Start region sqoop"
    os.system("/usr/bin/sqoop export --connect jdbc:mysql://193.205.211.69/monitoring --username monitoring --password  m0nit0ring --table region  --staging-table region_stage --clear-staging-table --export-dir /user/hdfs/out/region/"+region+"/part-* --input-fields-terminated-by '\\t' -m 1 --input-null-string '\\n' --input-null-non-string '\\n'")
    print "[RE] Ended region map/reducer"

def mapredVM():
    #VM mapred
    print "[VM] Remove vm folder if present"
    os.system('/usr/bin/hdfs dfs -rm -r /user/hdfs/out/vm/'+region)
    print "[VM] Start vm map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperVM.py -mapper mapperVM.py -file /home/xifi-mapReducer/reducerVM.py -reducer reducerVM.py -input /user/hdfs/'+region+'-working/'+region+'*-vm-*.txt -output /user/hdfs/out/vm/'+region);
    print "[VM] Start vm sqoop"
    os.system("/usr/bin/sqoop export --connect jdbc:mysql://193.205.211.69/monitoring --username monitoring --password  m0nit0ring --table vm  --staging-table vm_stage --clear-staging-table --export-dir /user/hdfs/out/vm/"+region+"/part-* --input-fields-terminated-by '\\t' -m 1 --input-null-string '\\n' --input-null-non-string '\\n'")
    print "[RE] ended vm map/reducer"
def mapredHostService():
    #host_service mapred
    print "[HS] Remove host_service folder if present"
    os.system('/usr/bin/hdfs dfs -rm -r /user/hdfs/out/host_service/'+region)
    print "[HS] Start host_service map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperHS.py -mapper mapperHS.py -file /home/xifi-mapReducer/reducerHS.py -reducer reducerHS.py -input /user/hdfs/'+region+'-working/'+region+'*-host_service-*.txt -output /user/hdfs/out/host_service/'+region);
    print "[HS] Start host_service sqoop"
    os.system("/usr/bin/sqoop export --connect jdbc:mysql://193.205.211.69/monitoring --username monitoring --password  m0nit0ring --table host_service  --staging-table host_service_stage --clear-staging-table --export-dir /user/hdfs/out/host_service/"+region+"/part-* --input-fields-terminated-by '\\t' -m 1 --input-null-string '\\n' --input-null-non-string '\\n'");
    print "[HS] ended host_service map/reducer";

actions=['region', 'vm', 'host_service']
threads = []

print "Starting.."
removeFolder();
moveFolder();

for act in actions:
    if act == 'region':
        thread = threading.Thread(target=mapredRegion);
        thread.start();
        threads.append(thread);
    elif act == 'vm':
        thread = threading.Thread(target=mapredVM);
        thread.start();
        threads.append(thread);
    elif act == 'host_service':
        thread = threading.Thread(target=mapredHostService);
        thread.start();
        threads.append(thread);    

# to wait until all three functions are finished


print "Waiting..."

for thread in threads:
    thread.join()
print "Complete."
#removeFolder()

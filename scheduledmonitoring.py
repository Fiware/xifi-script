#! /usr/bin/python
import threading
import os
import datetime
import time
region='XXXXXXX'
dbname='XXXXXXX'
username='XXXXXXX'
password='XXXXXXX'
dbIP="XXXXXXX"


def moveFolder():
    actualSample=datetime.date.fromordinal(datetime.date.today().toordinal()-1)
    actualTxt=actualSample.strftime('%d_%m_%Y')
    #now=datetime.datetime.now()
    #actualTxt=now.strftime('%d_%m_%Y');
    print "creating working-folder.."
    os.system('/usr/bin/hdfs dfs -mkdir /user/hdfs/'+region+'-working')
    print "Moving all dayly files.."
    os.system('/usr/bin/hdfs fsck /user/hdfs/'+region+' -delete');
    os.system('/usr/bin/hdfs fsck /user/hdfs/'+region+' -delete');
    os.system('/usr/bin/hdfs dfs -mv /user/hdfs/'+region+'/*'+actualTxt+'*.txt  /user/hdfs/'+region+'-working ')
    os.system('/usr/bin/hdfs fsck /user/hdfs/'+region+'-working -delete');
    os.system('/usr/bin/hdfs fsck /user/hdfs/'+region+'-working -delete');
    #Move the folder

def aggregateFile():
    print ("aggregating folder");
    os.system ('hadoop fs -text /user/hdfs/'+region+'-working/'+region+'*-region-*.txt        | hadoop fs -put - /user/hdfs/'+region+'-working/REGION.txt')
    os.system ('hadoop fs -text /user/hdfs/'+region+'-working/'+region+'*-vm-*.txt            | hadoop fs -put - /user/hdfs/'+region+'-working/VM.txt')
    os.system ('hadoop fs -text /user/hdfs/'+region+'-working/'+region+'*-host_service-*.txt  | hadoop fs -put - /user/hdfs/'+region+'-working/HOST_SERVICE.txt')
    os.system ('hadoop fs -text /user/hdfs/'+region+'-working/'+region+'*-host_co*.txt        | hadoop fs -put - /user/hdfs/'+region+'-working/HOST.txt')


def removeFolder():
    print ("removing working-folder")
    os.system('/usr/bin/hdfs dfs -rm -r -skipTrash /user/hdfs/'+region+'-working')
    #Remove the folder

def mapredRegion():
    #region mapred
    print "[RE] Remove region folder (if present)"
    os.system('/usr/bin/hdfs dfs -rm -r -skipTrash /user/hdfs/out/region/'+region)
    print "[RE] Start region map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperRegion.py -mapper mapperRegion.py -file /home/xifi-mapReducer/reducerRegion.py -reducer reducerRegion.py -input /user/hdfs/'+region+'-working/REGION.txt -output /user/hdfs/out/region/'+region);
    print "[RE] Start region sqoop"
    os.system("/usr/bin/sqoop export -D mapred.task.timeout=0  --connect jdbc:mysql://"+dbIP+"/"+dbname+" --username "+username+" --password  "+password+" --table region  --staging-table region_stage_"+region+" --clear-staging-table --export-dir /user/hdfs/out/region/"+region+"/part-* --input-fields-terminated-by '\\t' --input-lines-terminated-by '\\n' -m 1")
    print "[RE] Ended region map/reducer"

def mapredVM():
    #VM mapred
    print "[VM] Remove vm folder if present"
    os.system('/usr/bin/hdfs dfs -rm -r -skipTrash /user/hdfs/out/vm/'+region)
    print "[VM] Start vm map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperVM.py -mapper mapperVM.py -file /home/xifi-mapReducer/reducerVM.py -reducer reducerVM.py -input /user/hdfs/'+region+'-working/VM.txt -output /user/hdfs/out/vm/'+region);
    print "[VM] Start vm sqoop"
    os.system("/usr/bin/sqoop export -D mapred.task.timeout=0 --connect jdbc:mysql://"+dbIP+"/"+dbname+" --username "+username+" --password  "+password+" --table vm  --staging-table vm_stage_"+region+" --clear-staging-table --export-dir /user/hdfs/out/vm/"+region+"/part-* --input-fields-terminated-by '\\t' --input-lines-terminated-by '\\n' -m 1")
    print "[RE] ended vm map/reducer"
    
def mapredHostService():
    #host_service mapred
    print "[HS] Remove host_service folder if present"
    os.system('/usr/bin/hdfs dfs -rm -r -skipTrash /user/hdfs/out/host_service/'+region)
    print "[HS] Start host_service map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperHS.py -mapper mapperHS.py -file /home/xifi-mapReducer/reducerHS.py -reducer reducerHS.py -input /user/hdfs/'+region+'-working/HOST_SERVICE.txt -output /user/hdfs/out/host_service/'+region);
    print "[HS] Start host_service sqoop"
    os.system("/usr/bin/sqoop export -D mapred.task.timeout=0 --connect jdbc:mysql://"+dbIP+"/"+dbname+" --username "+username+" --password  "+password+" --table host_service  --staging-table host_service_stage_"+region+" --clear-staging-table --export-dir /user/hdfs/out/host_service/"+region+"/part-* --input-fields-terminated-by '\\t' --input-lines-terminated-by '\\n' -m 1");
    print "[HS] ended host_service map/reducer";
    
    
def mapredHost():
    #host_controller mapred
    print "[H] Remove host folder if present"
    os.system('/usr/bin/hdfs dfs -rm -r -skipTrash /user/hdfs/out/host/'+region)
    print "[H] Start host_controller/host_compute map/reducer"
    os.system('/usr/bin/hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/xifi-mapReducer/mapperH.py -mapper mapperH.py -file /home/xifi-mapReducer/reducerH.py -reducer reducerH.py -input /user/hdfs/'+region+'-working/HOST.txt -output /user/hdfs/out/host/'+region);
    print "[H] Start host_compute/host_controller sqoop"
    os.system("/usr/bin/sqoop export -D mapred.task.timeout=0 --connect jdbc:mysql://"+dbIP+"/"+dbname+" --username "+username+" --password  "+password+" --table host  --staging-table host_stage_"+region+" --clear-staging-table --export-dir /user/hdfs/out/host/"+region+"/part-* --input-fields-terminated-by '\\t' --input-lines-terminated-by '\\n' -m 1");
    print "[H] ended host_compute/host_controller map/reducer";


print "Starting.."
removeFolder();
moveFolder();
aggregateFile();
mapredRegion();
mapredVM();
mapredHost();
mapredHostService();
removeFolder()
print "Complete."


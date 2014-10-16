#Subscribe all services
#!/usr/bin/python
import sys, getopt
import requests
import json
import cStringIO
import datetime
import time
masterCBIP='10.0.32.20:1338'
servicesList=['cinder-api', 'cinder-scheduler', 'glance-api', 'glance-registry','nova-api', 'nova-cert', 'nova-conductor', 'nova-consoleauth', 'nova-novncproxy', 'nova-objectsore', 'nova-scheduler', 'quantum-dhcp-agent', 'quantum-l3-agent', 'quantum-metadata_agent', 'quantum-openvswitch-agent', 'quantum-server']
#servicesList=['cinder-api','cinder-scheduler']

def main(argv):
  regionName = ''
  ipAddress = ''
  try:
    opts, args = getopt.getopt(argv,"hr:a:",["region=","address="])
  except getopt.GetoptError:
    print 'subscribe.py -r <regionName> -a <ip:port>'
    sys.exit(2)
  if ( len(opts)==0):
    print 'subscribe.py -r <regionName> -a <ip:port>'
  for opt, arg in opts:
    if opt == '-h':
      print 'subscribe.py -r <regionName> -a <ip:port>'
      sys.exit()
    elif opt in ("-r", "--region"):
      regionName = arg
    elif opt in ("-a", "--address"):
      ipAddress = arg
    if (regionName!='' and ipAddress!=''):
      now = datetime.datetime.now()
      fileName=regionName+'_HS_'+now.strftime("%Y-%m-%d")
      fo = open(fileName, "wb")
      fo.write('Services subscribed for region: '+regionName+' IP: '+str(ipAddress)+'  ['+now.strftime("%Y-%m-%d %H:%M")+']\n');
      for srv in servicesList:
        time.sleep(1)
        srv2=srv.replace('-','_');
        agentUrl='http://'+ipAddress+'/NGSI10/subscribeContext'
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        data={"entities": [{"type": "host_service","isPattern": "true","id": regionName+':.*.:'+srv}],    "attributes": [],"reference": 'http://'+masterCBIP+'/ngsi10/notifyContext', "duration": "P10Y","notifyConditions": [{"type": "ONCHANGE","condValues": [srv2]}]}
        r=requests.post(agentUrl, headers=headers, json=data); 
        if r.status_code==200:
          r_j = json.loads(r.text)
          fo.write(regionName+' '+srv+': '+r_j['subscribeResponse']['subscriptionId']+'\n')
        else:
           fo.write(regionName+' '+srv+': ERROR\n')
      fo.close();

if __name__ == "__main__":
  main(sys.argv[1:])
import pycurl
import cStringIO
import time
region="regionName"
CBurl="x.y.v.z:1026"
hadoopMasterIP="a.b.c.b:5050"

agentUrl=CBurl+"/NGSI10/subscribeContext"
hadoopListenUrl="http://"+hadoopMasterIP+"/notify"

def subscribeRegion():
  c = pycurl.Curl()
  c.setopt(c.URL, agentUrl)
  c.setopt(c.HTTPHEADER, ['Content-Type: application/xml'])
  updated_body='<?xml version="1.0"?>\
  <subscribeContextRequest>\
    <entityIdList>\
      <entityId type="region" isPattern="true">\
        <id>'+region+'.*</id>\
      </entityId>\
    </entityIdList>\
      <attributeList>\
      <attribute>coreUsed</attribute>\
      <attribute>coreEnabled</attribute>\
      <attribute>coreTot</attribute>\
      <attribute>vmUsed</attribute>\
      <attribute>vmTot</attribute>\
      <attribute>hdUsed</attribute>\
      <attribute>hdTot</attribute>\
      <attribute>ramUsed</attribute>\
      <attribute>ramTot</attribute>\
    </attributeList>\
    <reference>'+hadoopListenUrl+'</reference>\
    <duration>P12Y</duration>\
    <notifyConditions>\
      <notifyCondition>\
        <type>ONTIMEINTERVAL</type>\
        <condValueList>\
          <condValue>PT300S</condValue>\
        </condValueList>\
      </notifyCondition>\
    </notifyConditions>\
  </subscribeContextRequest>';
  c.setopt(c.POSTFIELDS, updated_body)
  c.setopt(c.POST, 1)
  try:
    c.perform()
  except:
    print("Unable to connect to the contextBroker")

def subscribeHost_service():
  c = pycurl.Curl()
  c.setopt(c.URL, agentUrl)
  c.setopt(c.HTTPHEADER, ['Content-Type: application/xml'])
  updated_body='<?xml version="1.0"?>\
  <subscribeContextRequest>\
    <entityIdList>\
      <entityId type="host_service" isPattern="true">\
        <id>'+region+'.*</id>\
      </entityId>\
    </entityIdList>\
    <attributeList>\
      <attribute>cinder_api</attribute>\
      <attribute>cinder_schedule</attribute>\
      <attribute>glance_api</attribute>\
      <attribute>glance_registry</attribute>\
      <attribute>nova_api</attribute>\
      <attribute>nova_cert</attribute>\
      <attribute>quantum_dhcp_agent</attribute>\
      <attribute>quantum_l3_agent</attribute>\
      <attribute>nova_scheduler</attribute>\
      <attribute>nova_conductor</attribute>\
      <attribute>nova_consoleaut</attribute>\
      <attribute>nova_objectstore</attribute>\
      <attribute>quantum_server</attribute>\
      <attribute>quantum_openvswitch_agent</attribute>\
      <attribute>quantum_metadata_agent</attribute>\
      <attribute>nova_novncproxy</attribute>\
    </attributeList>\
    <reference>'+hadoopListenUrl+'</reference>\
    <duration>P12Y</duration>\
    <notifyConditions>\
      <notifyCondition>\
        <type>ONCHANGE</type>\
        <condValueList>\
          <condValue>_timestamp</condValue>\
        </condValueList>\
      </notifyCondition>\
    </notifyConditions>\
    <throttling>PT60S</throttling>\
  </subscribeContextRequest>';
  c.setopt(c.POSTFIELDS, updated_body)
  c.setopt(c.POST, 1)
  try:
    c.perform()
  except:
    print("Unable to connect to the contextBroker")

def subscribeVM():
  c = pycurl.Curl()
  c.setopt(c.URL, agentUrl)
  c.setopt(c.HTTPHEADER, ['Content-Type: application/xml'])
  updated_body='<?xml version="1.0"?>\
  <subscribeContextRequest>\
    <entityIdList>\
      <entityId type="vm" isPattern="true">\
        <id>'+region+'.*</id>\
      </entityId>\
    </entityIdList>\
    <attributeList>\
      <attribute>uid</attribute>\
      <attribute>cpuLoadPct</attribute>\
      <attribute>freeSpacePct</attribute>\
      <attribute>usedMemPct</attribute>\
      <attribute>host_name</attribute>\
      <attribute>host_id</attribute>\
    </attributeList>\
    <reference>'+hadoopListenUrl+'</reference>\
    <duration>P12Y</duration>\
    <notifyConditions>\
      <notifyCondition>\
        <type>ONTIMEINTERVAL</type>\
        <condValueList>\
          <condValue>PT60S</condValue>\
        </condValueList>\
      </notifyCondition>\
    </notifyConditions>\
  </subscribeContextRequest>';
  c.setopt(c.POSTFIELDS, updated_body)
  c.setopt(c.POST, 1)
  try:
    c.perform()
  except:
    print("Unable to connect to the contextBroker")

def subscribeHostCT():
  c = pycurl.Curl()
  c.setopt(c.URL, agentUrl)
  c.setopt(c.HTTPHEADER, ['Content-Type: application/xml'])
  updated_body='<?xml version="1.0"?>\
  <subscribeContextRequest>\
    <entityIdList>\
      <entityId type="host_controller" isPattern="true">\
        <id>'+region+'.*</id>\
      </entityId>\
    </entityIdList>\
    <attributeList>\
      <attribute>cpuLoadPct</attribute>\
      <attribute>freeSpacePct</attribute>\
      <attribute>usedMemPct</attribute>\
      <attribute>hostname</attribute>\
    </attributeList>\
    <reference>'+hadoopListenUrl+'</reference>\
    <duration>P12Y</duration>\
    <notifyConditions>\
      <notifyCondition>\
        <type>ONTIMEINTERVAL</type>\
        <condValueList>\
          <condValue>PT60S</condValue>\
        </condValueList>\
      </notifyCondition>\
    </notifyConditions>\
  </subscribeContextRequest>';
  c.setopt(c.POSTFIELDS, updated_body)
  c.setopt(c.POST, 1)
  try:
    c.perform()
  except:
    print("Unable to connect to the contextBroker")

def subscribeHostCP():
  c = pycurl.Curl()
  c.setopt(c.URL, agentUrl)
  c.setopt(c.HTTPHEADER, ['Content-Type: application/xml'])
  updated_body='<?xml version="1.0"?>\
  <subscribeContextRequest>\
    <entityIdList>\
      <entityId type="host_compute" isPattern="true">\
        <id>'+region+'.*</id>\
      </entityId>\
    </entityIdList>\
    <attributeList>\
      <attribute>cpuLoadPct</attribute>\
      <attribute>freeSpacePct</attribute>\
      <attribute>usedMemPct</attribute>\
      <attribute>hostname</attribute>\
    </attributeList>\
    <reference>'+hadoopListenUrl+'</reference>\
    <duration>P12Y</duration>\
    <notifyConditions>\
      <notifyCondition>\
        <type>ONTIMEINTERVAL</type>\
        <condValueList>\
          <condValue>PT60S</condValue>\
        </condValueList>\
      </notifyCondition>\
    </notifyConditions>\
  </subscribeContextRequest>';
  c.setopt(c.POSTFIELDS, updated_body)
  c.setopt(c.POST, 1)
  try:
    c.perform()
  except:
    print("Unable to connect to the contextBroker")

def main():
  subscribeRegion();
  time.sleep(1);
  subscribeHost_service();
  time.sleep(1);
  subscribeVM();
  time.sleep(1);
  subscribeHostCT();
  time.sleep(1);
  subscribeHostCP();

main()

xifi-script
===============
##General Information
###This repo is composed by these 4 scripts:
1. ngsi2cosmos:
    * this is the /etc/init.d script that performs the start/stop/status actions for the ngsi2cosmsos
    * IMPORTANT: check if the ngsi2cosmsos is under this path: */etc/monitoring/*
2. scheduledmonitoring.py:
    * script that must be scheduled as cronjob 
    * it should be scheduled as normal user
    * the final report should be a cron-job like [30 0 * * * python /etc/monitoring/scheduledmonitoring.py]
3.  subscribeHadoop.py:
    * this subscribes the ngsi2cosmos adapter to receive the data from the region CB
    * MUST be run only ONCE
4. testAPI.js:
    * it is a script that is useful in order to test the monitoring API
    * remember to require and set the ConsumerKey and the ConsumerSecret
    * node testAPI.js <username> <password> <apiPath>

###Additional configuration:
* ngsi2cosmos:

|name       |description |
|-----------|-----------|
|REGION|*set the region name*|
|DAEMON_PATH|*set the path to the ngsi2cosmos*|


* scheduledmonitoring.py:

|name       |description |
|-----------|-----------|
|region|*set the region name*|
|dbname|*set the db name*|
|username|*set the username*|
|password|*set the db user password*|


* subscribeHadoop.py:

|name       |description |
|-----------|-----------|
|region|*set the region name*|
|CBurl|*set the ContextBroker IP:Port*|
|hadoopMasterIP|*set the hadoopMaster IP:Port*|


* testAPI.js:

|name          |description |
|--------------|-----------|
|ConsumerKey|*set the oauth2 app-key*|
|ConsumerSecret|*set the oauth2 app-secret*|


##Contact

For any question, bug report, suggestion or feedback in general, please contact me: Attilio Broglio (abroglio at create-net dot org).

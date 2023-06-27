# snapmongo
Utility to snap Sharded MongoDB cluster to a target cluster
snapmongo
=========

Snap is a program to snapshot a mongo sharded database and load into another sharded cluster like QA or Staging

1. Requirements
    a. account and password on all servers involved with SUDO access permission.
    b. sshpass installed on all servers forking off shell commands. /etc/yum.repos.d/centos6.repo/ Turn on epel
    c. Mongo admin password and target Mongo DB user and password.
    d. Target DB created, or collections to restore dropped.
    e. Enough disk space for mongodump, scp, mongorestore on all servers involved.
    f. On production, make sure /backup/mongo/dbbackups has last nite or early morning backups across all shards.
    g. Make sure the source you defined for production servers is indeed 2ndary and space is available.
    h. On production, make sure /backup/snap1-4 has enough space.
    i. On target servers, make sure /backup/snap1-4 has enough space to copy the DB over.
    j. Make sure ALL servers defined are reachable(ssh) from where these python scripts are going to be run. Do some ssh to check.


2. Files
    a. mongos.txt  
    b. scptarget.txt 
    c. shardconfig.txt  
    d. sourceshard.txt  
    e. tshardconfig.txt
    f. .mypwd

3. Parameters

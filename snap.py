#!/usr/bin/python
################################################################################
##############
################################################################################
##############

from pymongo import *
import string,subprocess,datetime,commands,sys,time,os,os.path

SERVER=""
DB='admin'
USER="admin"
PASS=""
snap_home="/willy/"
shard_config_file=snap_home+"sourceshard.txt"
scptargetconfig=snap_home+"scptarget.txt"
mongosconfig=snap_home+"mongos.txt"
rescolconfig=snap_home+"shcollections"
resnoshardcol=snap_home+"noshcollections"
pw_file=snap_home+".mypwd"
shardf=""
shardnames=[]
hostnpath=[]
hostnpaths=[]
scptarget=[]
mngstarget=[]
rescolls=[]
resnoshcolls=[]
nshards=2
shard_data_path = "/backup/snap"
dbname=""
tdbname=""
mnguser=""
mngpwd=""

# Size of ums per shard. Now is about 25G
umsmax= 100.0
#restore node info
restore_path="/backup/snap"
restoreuser ="\\willy"
scpuser ="willy" # needs to escape 3 \times

#Get list of shardnames
def get_shardnames():
  shardf= open(shard_config_file,'r+')
  for hostname in shardf:
    hostname=hostname.strip()
    shardnames.append(hostname)
  scpt= open(scptargetconfig,'r+')
  for hostname in scpt:
    hostname=hostname.strip()
    scptarget.append(hostname)
  mngs= open(mongosconfig,'r+')
  for hostname in mngs:
    hostname=hostname.strip()
    mngstarget.append(hostname)
  rescol= open(rescolconfig,'r+')
  for hostname in rescol:
    hostname=hostname.strip()
    rescolls.append(hostname)
  resnosh= open(resnoshardcol,'r+')
  for hostname in resnosh:
    hostname=hostname.strip()
    resnoshcolls.append(hostname)
  shardf.close()
  scpt.close()
  mngs.close()
  rescol.close()
  resnosh.close()
# get non sharded collections
def getnonshardcolls(hostname):
  print "getnonshardcolls " + hostname
  db_con = Connection(hostname)
  metdb=database.Database(db_con,DB)
  metdb.authenticate(USER,PASS)

  res = metdb.getSiblingDB("ums").getCollectionNames()
  print res

  return True

def checksecond():
  for hostname in shardnames:
      print "checksecond " + hostname
      db_con = Connection(hostname)
      metdb=database.Database(db_con,DB)
      metdb.authenticate(USER,PASS)

      replStatus = db_con.admin.command("replSetGetStatus")
      if ( replStatus['myState'] != 2):
        print hostname +' ' + str(replStatus['myState'])
        return False

  return True

def checkspacedfR(hostname,shard_data_path):
  print "checkspacedfR"
  print hostname + " " + shard_data_path
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  diskusage_cmd= "df -h " + shard_data_path + " | awk '{print $3}' "
  print xxyy + " " + diskusage_cmd
  #scppwd(hostname)
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh','-t',xxyy, diskusage_cmd], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  #get the size and ignore folder path from tuple output
  data_size=string.rsplit(dbsize_out[0])
  data_dir_size=convert_to_gb(data_size[len(data_size)-1])
  print 'Restore ' + hostname +'=' + str(data_dir_size)
  return data_dir_size

def checkspace(hostname,shard_data_path):
  print "checkspace"
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  diskusage_cmd= "cat ~/.mypwd |sudo -S du -sh " + shard_data_path + " | awk '{print $1}' "
  #scppwd(hostname)
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh','-t',xxyy, diskusage_cmd], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  #get the size and ignore folder path from tuple output
  data_size=string.rsplit(dbsize_out[0])
  data_dir_size=convert_to_gb(data_size[len(data_size)-1])
  print '/backup/snap for hostname ' + hostname +'=' + str(data_dir_size)
  return data_dir_size

def checkspacedf(hostname,shard_data_path):
  print "checkspacedf"
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  diskusage_cmd= "cat ~/.mypwd |sudo -S df -h " + shard_data_path + " | awk '{print $3}' "
  #scppwd(hostname)
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh','-t',xxyy, diskusage_cmd], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  #get the size and ignore folder path from tuple output
  data_size=string.rsplit(dbsize_out[0])
  data_dir_size=convert_to_gb(data_size[len(data_size)-1])
  print '/backup for hostname ' + hostname +'=' + str(data_dir_size)
  return data_dir_size

def mngdump(hostname,shard_data_path):
  print "mongodump"
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  dmp_cmd= "cat ~/.mypwd |sudo -S time mongodump --dbpath /backup/mongo/dbbackups/"+dbname+"/ -o " + shard_data_path
  print time.strftime("%c")+" "+dmp_cmd
  #scppwd(hostname)
  #
  #csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh', '-t',xxyy, dmp_cmd], stdout=subprocess.PIPE)
  #dbsize_out=csp.communicate()
  #print dbsize_out
  #
  #data_size=string.rsplit(dbsize_out[0])
  #data_dir_size=convert_to_gb(data_size[len(data_size)-1])
  #print 'time for dump ' + hostname +'= ' + str(data_dir_size)
  return
#
def chmodmngdump(hostname,shard_data_path):
  print "chmodmongodump"
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  dmp_cmd= "cat ~/.mypwd |sudo -S chmod -R 777 " + shard_data_path
  print time.strftime("%c")+" "+dmp_cmd
  #
  #csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh', '-t',xxyy, dmp_cmd], stdout=subprocess.PIPE)
  #dbsize_out=csp.communicate()
  #print dbsize_out
  #
  return

#restorecolls
def colbycolnosh():
  idx =0
  for colnm in resnoshcolls:
    idx =0
    for target in scptarget:
      print "hostnpath "+ hostnpath[idx][0] + " "+ hostnpath[idx][1]
      restorecolls(mngstarget[idx],colnm,hostnpath[idx][0],hostnpath[idx][1],idx)
      idx = idx +1
#mongorestore
def colbycol():
  idx =0
  for colnm in rescolls:
    idx =0
    for target in scptarget:
      print "hostnpath "+ hostnpath[idx][0] + " "+ hostnpath[idx][1]
      restorecolls(mngstarget[idx],colnm,hostnpath[idx][0],hostnpath[idx][1],idx)
      idx = idx +1
#restorecolls
def restorecolls(hostname,colnm,target,tpath,id):
  print "restore " + hostname + " " + colnm + " " + target + " " +tpath
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+target

  if (":" in hostname):
    hname = hostname.split(':')[0]
    pn = hostname.split(':')[1]
  else:
    hname = hostname
    pn = "27017"
  zzyy = hname + " --port "+pn+" --username "+mnguser+" --password \""+mngpwd+"\" --collection "+colnm+" "+tpath+dbname+"/"+colnm+".bson --db " + tdbname + " --noIndexRestore -vvv "
  #scppwd(target)
  scp_cmd= "/usr/bin/mongorestore --host " + zzyy + " > "+colnm+".out" +" 2>&1"
  print time.strftime("%c")+" "+scp_cmd
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh', '-t',xxyy, scp_cmd], stdout=subprocess.PIPE)
  if (id == len(scptarget)):
  #Block until done the last shard load assuming last one will end last
  resp=csp.communicate()
  print resp
  #
  return

def scpdump(hostname,shard_data_path, target,tpath):
  print "scp " + hostname + " " + shard_data_path + " " + target
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+target
  zzyy = scpuser + '@'+hostname
  #scppwd(target)
  #scp_cmd= "cat ~/.mypwd |sudo -S time scp -rp " + shard_data_path + " "+zzyy+":"+tpath
  scp_cmd= "/usr/bin/sshpass -f ~/.mypwd scp -r " + zzyy +":"+shard_data_path + " "+tpath
  print time.strftime("%c")+" "+scp_cmd
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh', '-t',xxyy, scp_cmd], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  #
  #data_size=string.rsplit(dbsize_out[0])
  #print 'time for dump ' + hostname +'= ' + str(data_dir_size)
  return

# copy pwd file over
def scppwd(hostname):
  print "scp pwd file " + hostname
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname+":~/"
  scp_cmd= pw_file
  print scp_cmd + " " + xxyy
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'scp',scp_cmd,xxyy], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  #
  #data_size=string.rsplit(dbsize_out[0])
  #print 'time for dump ' + hostname +'= ' + str(data_dir_size)

def cleardump(hostname,shard_data_path):
  print "cleardump"
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  dmp_cmd= "cat ~/.mypwd |sudo -S rm -rf " + shard_data_path+ dbname
  print dmp_cmd
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh', '-t',xxyy, dmp_cmd], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  return

def convert_to_gb(v_size):
  #find out if size is mega or kilobyte. get suffix of size M or K.
  size_suffix=v_size[len(v_size)-1]
  fsize=float(v_size.rstrip('MGKT'))
  #convert to GB accordinly
  if size_suffix =='M':
     size_gb=fsize/1048576
  elif size_suffix =='K':
     size_gb=fsize/1073741824
  elif size_suffix =='T':
    size_gb=fsize*1024
  else:
     size_gb=fsize

  #print 'size_gb =' ,size_gb
  return size_gb


#
def gentargetshardinfo():
  print "gentargetshardinfo"
  print time.strftime("%c")
  xxyy = mngstarget[0]+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + PASS + " --eval " + "\"printjson(db.printShardingStatus(true))\" "  + " > afterrestore.out"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
  if os.path.isfile(snap_home+'afterrestore.out'):
    return True
  return False
#



def main():
  print "Snap Main"
  print "----------------------------------------------------------------"
  global dbname, tdbname, mnguser, mngpwd
  global hostnpath
  if (len(sys.argv) == 5):
    dbname = sys.argv[1]
    tdbname = sys.argv[2]
    mnguser = sys.argv[3]
    mngpwd = sys.argv[4]
    print "Snap Shot of "+dbname +" restore to target "+tdbname+" user "+mnguser
  else:
    print "Please provide 1.database name 2. target 3. mongo user 4. password"
    return
  get_shardnames()
  #
  if (checksecond()):
    print 'true'
  else:
    print 'There are Primary nodes here !'
    return


  for hostname in shardnames:
    aa = hostname.rsplit('.')
    nsh = int(aa[0][-1])
    if (nsh == 0):
      shardpath = shard_data_path+"4/"
      print shardpath
      bksz = checkspacedf(hostname,shardpath)
      if (bksz < umsmax):
        print '/backup size too small'
        return
      bksz = checkspace(hostname,shardpath)
      if (bksz > 100.0):
        print 'please clear previous space 4 '
        return
      mngdump(hostname,shardpath)
      chmodmngdump(hostname,shardpath)
      bksz1 = checkspace(hostname,shardpath)
      print ' After dump snap size ' + str(bksz1)
      #scp to a consolidated node, check size first
      rpath = restore_path+"4/"
      hostnpath.append((scptarget[3],rpath))
      ressz1 = checkspacedfR(scptarget[3],rpath)
      print ' Restore size ' + str(ressz1)
      if (bksz1 > ressz1):
        print 'please clear Restore space 4'
        return
      #scpdump(hostname,shardpath+dbname,scptarget[3],rpath)
    elif (nsh <= 3 ):
      shardpath = shard_data_path+"1/"
      print shardpath
      bksz = checkspacedf(hostname,shardpath)
      if (bksz < umsmax):
        print '/backup size too small'
        return
      bksz = checkspace(hostname,shardpath)
      if (bksz > 100.0):
        print 'please clear previous space 1 '
        return
      mngdump(hostname,shardpath)
      chmodmngdump(hostname,shardpath)
      bksz1 = checkspace(hostname,shardpath)
      print ' After dump snap size ' + str(bksz1)
      #scp to a consolidated node, check size first
      rpath = restore_path+"1/"
      hostnpath.append((scptarget[0],rpath))
      ressz1 = checkspacedfR(scptarget[0],rpath)
      print ' Restore size ' + str(ressz1)
      if (bksz1 > ressz1):
        print 'please clear Restore space 1'
        return
      #scpdump(hostname,shardpath+dbname,scptarget[0],rpath)
      # remember to restore with no indexing json files

      #remove copied db to clear space
      #cleardump(hostname,shardpath)

    elif (nsh <= 6 ):
      shardpath = shard_data_path+"2/"
      print shardpath
      bksz = checkspacedf(hostname,shardpath)
      if (bksz < umsmax):
        print '/backup size too small'
        return
      bksz = checkspace(hostname,shardpath)
      if (bksz > 100.0):
        print 'please clear previous space 2'
        return
      mngdump(hostname,shardpath)
      chmodmngdump(hostname,shardpath)
      bksz1 = checkspace(hostname,shardpath)
      print ' After dump snap size ' + str(bksz1)
      #scp to a consolidated node, check size first
      rpath = restore_path+"2/"
      hostnpath.append((scptarget[1],rpath))
      ressz1 = checkspacedfR(scptarget[1],rpath)
      print ' Restore size ' + str(ressz1)
      if (bksz1 > ressz1):
        print 'please clear Restore space 2'
        return

      #scpdump(hostname,shardpath+dbname,scptarget[1],rpath)
      # remember to restore with no indexing json files

      #remove copied db to clear space
      #cleardump(hostname,shardpath)
    else:
      shardpath = shard_data_path+"3/"
      print shardpath
      bksz = checkspacedf(hostname,shardpath)
      if (bksz < umsmax):
        print '/backup size too small'
        return
      bksz = checkspace(hostname,shardpath)
      if (bksz > 100.0):
        print 'please clear previous space 3'
        return
      mngdump(hostname,shardpath)
      chmodmngdump(hostname,shardpath)
      bksz1 = checkspace(hostname,shardpath)
      print ' After dump snap size ' + str(bksz1)
      #scp to a consolidated node, check size first
      rpath = restore_path+"3/"
      hostnpath.append((scptarget[2],rpath))
      ressz1 = checkspacedfR(scptarget[2],rpath)
      print ' Restore size ' + str(ressz1)
      if (bksz1 > ressz1):
        print 'please clear Restore space 3'
      #scpdump(hostname,shardpath+dbname,scptarget[2],rpath)


    #for each collection in shcollection file, call mongorestore
  print "hostnpaths: "
  print hostnpath
  colbycol()
  #Finally, restore any collection not listed in shcollection
  colbycolnosh()
  #Sanity Check
  gentargetshardinfo()

  #Finally, create indexes


if __name__=="__main__":
  main()

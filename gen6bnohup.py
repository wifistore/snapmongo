#!/usr/bin/python
################################################################################
# Goal of this file gensplitchk is to generate split chunk and move chunk
# commands based on number of originator shards, target shards, shard indexes
# of each collection.
##############

from pymongo import *
import string,subprocess,datetime,commands, shutil, os, os.path, time

SERVER=""
DB='admin'
USER="admin"
PASS="XXXX"
snap_home="/willy/perf/"
shardconfig_file=snap_home+"shardconfig.txt"
tshardconfig_file=snap_home+"tshardconfig.txt"
pw_file=snap_home+".mypwd"
shardf=""
shardnames=[]
tshardnames=[]
collectionnames=[]
shardinfo=[]
uindex=[]
noshcollnames=[]
#
nshards =0
ntshards =0
shard_data_path = "/backup/snap"
dbname =""
tdbname =""
mongoshost =""
mongostarget =""
mongoadminpwd="W1r4dY3H7"

# Size of ums per shard. Now is about 25G
umsmax= 30.0
#restore node info
restoreuser ="cloud-user"

#mspline for multi line composite index
#} on : shard0003 Timestamp(15, 66)
def mspline(line):
  bidx =0
  eidx =0
  i = 0
  splitline =""
  mvline =""
  bidx = len("} on : ")
  eidx = line.index(" Timestamp")

  splitline = (line[bidx:eidx])
  for x in shardnames:
    if splitline == x:
      break
    else:
      i = i+1
  return tshardnames[i]


#spline
def spline(line,collnm, rrobin, ntom):
  bidx =0
  eidx =0

  splitline =[]
  mvline =""
  bidx = line.index("{ \"")
  eidx = line.index("\" :")
  eidx2 = line.index("}")

  # process 1st line minKey
  if ( "minKey" in line):    # one extra } for minkey
    cmd = "db.runCommand({split:\""+collnm+"\"" +",middle:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}});\n"
  #generate moveChunk
    if (shardnames[0] in line):
      mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}, to:"+"\""+tshardnames[0]+"\"});\n"
    elif (shardnames[1] in line):
      mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}, to:"+"\""+tshardnames[1]+"\"});\n"
    elif (shardnames[2] in line):
      mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}, to:"+"\""+tshardnames[2]+"\"});\n"
    elif (shardnames[3] in line):
      mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}, to:"+"\""+tshardnames[3]+"\"});\n"
    elif (shardnames[4] in line):
      mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}, to:"+"\""+tshardnames[4]+"\"});\n"
    elif (shardnames[5] in line):
      mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}}, to:"+"\""+tshardnames[5]+"\"});\n"
  else:
    cmd = "db.runCommand({split:\""+collnm+"\"" +",middle:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}});\n"
  #generate moveChunk
    if (ntom): #Round Robin
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[(rrobin%ntshards)]+"\"});\n"
    else: # this is N to N
      if (shardnames[0] in line):
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[0]+"\"});\n"
      elif (shardnames[1] in line):
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[1]+"\"});\n"
      elif (shardnames[2] in line):
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[2]+"\"});\n"
      elif (shardnames[3] in line):
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[3]+"\"});\n"
      elif (shardnames[4] in line):
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[4]+"\"});\n"
      elif (shardnames[5] in line):
        mvline = "db.runCommand({moveChunk:\""+collnm+"\"" +",find:{"+line[bidx+3:eidx]+ line[eidx+1:eidx2]+"}, to:"+"\""+tshardnames[5]+"\"});\n"

  splitline.append(cmd)
  splitline.append(mvline)
  return splitline

# foreach db.collection, get shard keys info, skip minKey, get next line,
# till it reach # maxKey, then find next collection again until eof
# Goal is to generate shardCollection cmd, then each collection chunk split cmd
def  ldcolls():
  global collectionnames
  lfn = open(snap_home+'shcollections',"r")
  for line in lfn:
    x = line.strip('\n')
    collectionnames.append(x)
  lfn.close()

def  processshardinfo():
  global collectionnames
  global shardinfo
  foundcoll = False
  foundmin = False
  foundmax = False
  nextcoll = False
  minkeynext = False
  mminkeynext = False
  multishardkeys= False
  shardindex =""
  dbn = dbname + "."
  collnm =""
  dbcollnm =""
  shadcollline=""
  splitinfo=[]
  splitline=[]
  msplitline=[]
  shindex=[]
  msplt = ""
  compindex = ""
  compindexshard =""
  compindexmv =""
  ntom = False
  rrobin = 1  #used for mton = True and round robin nshards placement splits

  print "gensourceshardinfo"
  print time.strftime("%c")
  # if source number of shards is different to target number of shards
  if (nshards != ntshards):
    ntom = True

  rfn = open(snap_home+'shout',"r")
  wfn = open(snap_home+'shindex',"w")
  ufn = open(snap_home+'uidx',"w")
 #keep track of sharded collection names used later for non sharded collections
  sfn = open(snap_home+'shcollections',"w")

  for line in rfn:
   if (dbn in line ):
     foundcoll = True
     dbcollnm=""
     collnm=""
     dbcollnm = line[2:len(line)-1]
     collnm = dbcollnm.rsplit(".",1)
     collectionnames.append(collnm[1])
     sfn.write(collnm[1]+'\n')
     if not os.path.exists(snap_home+collnm[1]):
       os.makedirs(snap_home+collnm[1])
     shutil.copy(snap_home+"crchk",snap_home+collnm[1])
     c1fn = open(snap_home+collnm[1]+"/sp"+"1.js","w")
     c2fn = open(snap_home+collnm[1]+"/sp"+"2.js","w")
     c3fn = open(snap_home+collnm[1]+"/sp"+"3.js","w")
     c4fn = open(snap_home+collnm[1]+"/sp"+"4.js","w")
     c5fn = open(snap_home+collnm[1]+"/sp"+"5.js","w")
     c6fn = open(snap_home+collnm[1]+"/sp"+"6.js","w")
     m1fn = open(snap_home+collnm[1]+"/mv"+"1.js","w")
     m2fn = open(snap_home+collnm[1]+"/mv"+"2.js","w")
     m3fn = open(snap_home+collnm[1]+"/mv"+"3.js","w")
     m4fn = open(snap_home+collnm[1]+"/mv"+"4.js","w")
     m5fn = open(snap_home+collnm[1]+"/mv"+"5.js","w")
     m6fn = open(snap_home+collnm[1]+"/mv"+"6.js","w")
   elif (foundcoll):  # get shard index info
     shardindex = line[14:len(line)-1]
     shardinfo.append((collnm[1],shardindex))
     shindex = shardindex.split('"')
     ufn.write(collnm[1])
     for i in shindex:
       if (not ':' in i and (not '{' in i) and (not ':' in i) and (not '}' in i)):
         ufn.write(' '+i)
     ufn.write('\n')
     #end shard index info
     foundcoll = False
     minkeynext = True
     shardcollline = "sh.shardCollection(\""+tdbname+'.'+collnm[1] + "\"," +shardindex +");\n"
     wfn.write(shardcollline)
     if (shardindex.count(',') > 1):
       multishardkeys = True
     else:
       multishardkeys = False
   # get shard index info finished here.
   # single line shard keys
   elif ("-->>" in line and "on :" in line and minkeynext ):
   # now get objid and get straight  to each shards if it is N to N
     splitline = spline(line,tdbname+'.'+collnm[1],rrobin,ntom)

     rrobin = rrobin+1
     if (tshardnames[0] in splitline[1]):
       c1fn.write(splitline[0])
       m1fn.write(splitline[1])
     elif(tshardnames[1] in splitline[1]):
       c2fn.write(splitline[0])
       m2fn.write(splitline[1])
     elif(tshardnames[2] in splitline[1]):
       c3fn.write(splitline[0])
       m3fn.write(splitline[1])
     elif(tshardnames[3] in splitline[1]):
       c4fn.write(splitline[0])
       m4fn.write(splitline[1])
     elif(tshardnames[4] in splitline[1]):
       c5fn.write(splitline[0])
       m5fn.write(splitline[1])
     else:
       c6fn.write(splitline[0])
       m6fn.write(splitline[1])
     del splitline[:] #clear list for next
     shindex=[] #clear list for next
     if ("maxKey" in line):
       minkeynext = False
       c1fn.close()
       c2fn.close()
       c3fn.close()
       c4fn.close()
       c5fn.close()
       c6fn.close()
       m1fn.close()
       m2fn.close()
       m3fn.close()
       m4fn.close()
       m5fn.close()
       m6fn.close()
   # multi line shard keys
   elif ("} -->> {\n" in line and multishardkeys and minkeynext):
     mminkeynext = True
   elif ("Timestamp" in line and mminkeynext): # end of composit record
       #generate Split and Move
       compindex = "db.runCommand({split:\""+tdbname+'.'+collnm[1]+"\"" +",middle:{"+msplt+"}});\n"
       #print compindex
       msplitline.append(compindex)
       compindexshard = mspline(line)

       if (ntom): #Round Robin
         compindexmv = "db.runCommand({moveChunk:\""+tdbname+'.'+collnm[1]+"\"" +",find:{"+msplt+"}, to:"+"\""+tshardnames[(rrobin%ntshards)]+"\"});\n"
       else: # this is N to N
         compindexmv = "db.runCommand({moveChunk:\""+tdbname+'.'+collnm[1]+"\"" +",find:{"+msplt+"}, to:"+"\""+compindexshard+"\"});\n"

       msplitline.append(compindexmv)
       msplt = ""
       compindex = ""
       compindexshard = ""
       compindexmv = ""
       mminkeynext = False
       rrobin = rrobin+1
       #generate Split and Move

       if (tshardnames[0] in msplitline[1]):
         c1fn.write(msplitline[0])
         m1fn.write(msplitline[1])
       elif(tshardnames[1] in msplitline[1]):
         c2fn.write(msplitline[0])
         m2fn.write(msplitline[1])
       elif(tshardnames[2] in msplitline[1]):
         c3fn.write(msplitline[0])
         m3fn.write(msplitline[1])
       elif(tshardnames[3] in msplitline[1]):
         c4fn.write(msplitline[0])
         m4fn.write(msplitline[1])
       elif(tshardnames[4] in msplitline[1]):
         c5fn.write(msplitline[0])
         m5fn.write(msplitline[1])
       else:
         c6fn.write(msplitline[0])
         m6fn.write(msplitline[1])
       del msplitline[:]
   elif (multishardkeys and "maxKey" in line and mminkeynext):
       minkeynext = False
       mminkeynext = False
       c1fn.close()
       c2fn.close()
       c3fn.close()
       c4fn.close()
       c5fn.close()
       c6fn.close()
       m1fn.close()
       m2fn.close()
       m3fn.close()
       m4fn.close()
       m5fn.close()
       m6fn.close()
   elif (mminkeynext and minkeynext):
       msplt =  msplt + line.strip()




   shardindex=""
   shardcollline=""

  wfn.close()
  rfn.close()
  ufn.close()
  sfn.close()
  c1fn.close()
  c2fn.close()
  c3fn.close()
  c4fn.close()
  c5fn.close()
  c6fn.close()
  m1fn.close()
  m2fn.close()
  m3fn.close()
  m4fn.close()
  m5fn.close()
  m6fn.close()

#
def gencrchk():
  xfn = open(snap_home+'crchk',"w")
  cmd ="#\ntime mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./sp1.js\n"
  cmd2 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./sp2.js\n"
  cmd3 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./sp3.js\n"
  cmd4 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./sp4.js\n"
  cmd5 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./sp5.js\n"
  cmd6 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./sp6.js\n"
  cmd7 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./mv1.js\n"
  cmd8 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./mv2.js\n"
  cmd9 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./mv3.js\n"
  cmd10 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./mv4.js\n"
  cmd11 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./mv5.js\n"
  cmd12 = "time mongo "+mongostarget+"/admin -uadmin -p\""+mongoadminpwd+"\" ./mv6.js\n"

  xfn.write(cmd)
  xfn.write(cmd2)
  xfn.write(cmd3)
  xfn.write(cmd4)
  xfn.write(cmd5)
  xfn.write(cmd6)
  xfn.write(cmd7)
  xfn.write(cmd8)
  xfn.write(cmd9)
  xfn.write(cmd10)
  xfn.write(cmd11)
  xfn.write(cmd12)
  xfn.close()
  os.chmod("crchk", 0744)

# shutdownbalancer
def shutdownbalancer():
  print "shutdownbalancer"
  print time.strftime("%c")
  xxyy = mongostarget+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " --eval " + "\"sh.setBalancerState(false)\" "
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #


#
def splitandmove(col):
  print "splitandmove"
  print time.strftime("%c")
  gen_cmd = "cd "+col+"; ./crchk"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
# enableSharding
def shardDB():
  shardcollline=""
  print "shardDB"
  print time.strftime("%c")
  xxyy = mongostarget+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " --eval " + "\"sh.enableSharding('"+tdbname+"');\""
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #

# shardCollections
def shardCollections():
  shardcollline=""
  print "shardCollections"
  print time.strftime("%c")
  xxyy = mongostarget+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " ushindex.js"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
# Only doing this because we need to find out unique shard index
def gensourceindexinfo():
  print "gensourceindexinfo"
  print time.strftime("%c")
  xxyy = mongoshost+ '/admin'

  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " --eval " + "\"db=db.getSiblingDB('"+ dbname+ "');idx=db.system.indexes.find({unique:true},{name:1,ns:1});while(idx.hasNext()){printjson(idx.next());}\""  + " > uidxout"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
  if (not os.path.isfile(snap_home+'uidxout')):
    return False

  rfn = open(snap_home+'uidxout',"r")
  wfn = open(snap_home+'uidxout2',"w")
  for aa in rfn:
    if (not ("MongoDB" in aa) and not("connecting" in aa)):
      y = aa.strip()
      y = aa.strip(' }\n')
      y = aa.strip(', ')
      y = aa.split('"')
      for x in y:
        j = x.split('_')
        #print j
        for i in j:
          if ( not ('_' in i) and not('1' in i ) and not(':' in i ) and not("ns" == i) and not("name" in i) and not ('{' in i) and not('}' in i) and not(',' in i)):
            if ('.' in i):
              x = i.split('.')
              wfn.write(x[1])
            else:
              wfn.write(' '+i)

      wfn.write('\n')
  wfn.close()
  rfn.close()
  #now check if unique index, if so, generate shard collection command

  r2fn = open(snap_home+'uidx',"r")

  for ii in r2fn:
    r1fn = open(snap_home+'uidxout2',"r")
    #print ii
    for jj in r1fn:
      if ii in jj:
        #print 'found'
        #print ii
        uindex.append(ii.split(' ')[0])
    r1fn.close()


  #print uindex
  # modify shindex to ushindex.js to reflect unique shard index
  r3fn = open(snap_home+'shindex',"r")
  w4fn = open(snap_home+'ushindex.js',"w")
  for i in r3fn:
    for x in uindex:
      if '.'+x+'"' in i:
        i=i.replace("});","}, true);")
        break
    w4fn.write(i)

  r3fn.close()
  w4fn.close()
  r1fn.close()
  r2fn.close()


  return True
#
def noshardcollection():
  print "noshardcollection"
  print time.strftime("%c")
  xxyy = mongoshost+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " --eval " + "\"printjson(db.getSiblingDB('"+ dbname+ "').getCollectionNames())\" "  + " > noshcollections1"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
  noshf = open(snap_home+'noshcollections1',"r")
  noshf2 = open(snap_home+'noshcollections',"w")
  for col in noshf:
    col=col.replace('"','').strip()
    col=col.rstrip(',')
    if ( not('connecting to' in col) and not('MongoDB' in col) and not('[' in col) and not (']' in col)and not ('system' in col) and not(col in collectionnames )):
      noshf2.write(col+'\n')
  noshf2.close()
  noshf.close()
  return True

#
def gensourceshardinfo():
  print "gensourceshardinfo"
  print time.strftime("%c")
  xxyy = mongoshost+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " --eval " + "\"printjson(db.printShardingStatus(true))\" "  + " > shout"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
  if os.path.isfile(snap_home+'shout'):
    return True
  return False
#
def gentargetshardinfo():
  print "gentargethardinfo"
  print time.strftime("%c")
  xxyy = mongostarget+ '/admin'
  gen_cmd = "/usr/bin/mongo " + xxyy + " -u admin -p " + mongoadminpwd + " --eval " + "\"printjson(db.printShardingStatus(true))\" "  + " > tshout"
  print "CMD " + gen_cmd
  csp = subprocess.Popen(gen_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell = True)
  op=csp.communicate()
  print op
  #
  if os.path.isfile(snap_home+'tshout'):
    return True
  return False
#
def get_allnames():
  global nshards
  global ntshards
  global dbname
  global tdbname
  global mongoshost
  global mongostarget
  #n = int(raw_input('From how many shards in the Source ?'))
  nshards = 6
  #n = int(raw_input('From how many shards in the Target ?'))
  ntshards = 6
  #dbnm = raw_input('What is the Source database name ?')
  dbname = "ums"
  #dbnm = raw_input('What is the Target database name ?')
  tdbname = "ums"
  #shost = raw_input('What is the Source Mongos node ?')
  mongoshost = "hostname01"
  #thost = raw_input('What is the Target Mongos node ?')
  mongostarget = "targethost:27017"
  print "Please Verify:"

  print 'Source #of shards ' +str(nshards) + ' Target #of shards ' + str(ntshards) + ' Source DBname: ' + dbname +' Source mongos: ' + mongoshost + ' Target mongos: ' + mongostarget + ' Target DBname: ' +tdbname

  print 'Source Shard Names '+ ', '.join(shardnames)
  print 'Target Shard Names '+ ', '.join(tshardnames)

  #verify = int(raw_input('Enter 1 to Proceed :'))
  verify = 1
  if (verify == 1):
    return True
  return False

#Get list of shardnames
def get_shardnames():
  shardf= open(shardconfig_file,'r+')
  tshardf= open(tshardconfig_file,'r+')
  for hostname in shardf:
    hostname=hostname.strip()
    shardnames.append(hostname)
  for hostname in tshardf:
    hostname=hostname.strip()
    tshardnames.append(hostname)
  shardf.close()
  tshardf.close()

def checksecond():
#  shardf=get_shardnames()
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
#
def checkspacedfR(hostname,shard_data_path):
  print "checkspacedfR"
  pwd = '-f'+pw_file
  xxyy = restoreuser + '@'+hostname
  diskusage_cmd= "df -h " + shard_data_path + " | awk '{print $4}' "
  #
  csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh','-t',xxyy, diskusage_cmd], stdout=subprocess.PIPE)
  dbsize_out=csp.communicate()
  print dbsize_out
  #get the size and ignore folder path from tuple output
  data_size=string.rsplit(dbsize_out[0])
  data_dir_size=convert_to_gb(data_size[len(data_size)-1])
  print 'IAAS Restore ' + hostname +'=' + str(data_dir_size)
  return data_dir_size

def checkspace(hostname,shard_data_path):
  print "checkspace"
  pwd = '-f'+pw_file
  xxyy = 'willy@'+hostname.rstrip('.')
  diskusage_cmd= "cat ~/.mypwd |sudo -S du -sh " + shard_data_path + " | awk '{print $1}' "
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
  xxyy = 'willy@'+hostname.rstrip('.')
  diskusage_cmd= "cat ~/.mypwd |sudo -S df -h " + shard_data_path + " | awk '{print $3}' "
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
  xxyy = 'willy@'+hostname.rstrip('.')
  dmp_cmd= "cat ~/.mypwd |sudo -S time mongodump --dbpath /backup/mongo/dbbackups/"+dbname+"/ -o " + shard_data_path
  print dmp_cmd
  #
  #csp = subprocess.Popen(['/usr/bin/sshpass',pwd,'ssh', '-t',xxyy, dmp_cmd], stdout=subprocess.PIPE)
  #dbsize_out=csp.communicate()
  #print dbsize_out
  #
  #data_size=string.rsplit(dbsize_out[0])
  #print 'time for dump ' + hostname +'= ' + str(data_dir_size)
  return

def cleardump(hostname,shard_data_path):
  print "cleardump"
  pwd = '-f'+pw_file
  xxyy = 'willy@'+hostname.rstrip('.')
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


def main():
  print "Main gensplitchk"
  print "--------------------------------------------------"
  get_shardnames()
  if (not get_allnames()):
    return
  #
  #if (not gensourceshardinfo()):
  #  return

  #gencrchk()
  #processshardinfo()
  #if (not noshardcollection()):
  #  return
  #if (not gensourceindexinfo()):
  #  return
  # turn off load balancer
  shutdownbalancer()
  # shard collections
  #shardDB()
  #shardCollections()
  #
  ldcolls()
  for col in collectionnames:
    splitandmove(col)
  # Done
  gentargetshardinfo()
  print "Done"
  print time.strftime("%c")

if __name__=="__main__":
  main()


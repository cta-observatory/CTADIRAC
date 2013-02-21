#!/usr/bin/env python
import DIRAC
import os


def setRunNumber( optionValue ):
  global run_number
  run_number = optionValue.split('ParametricParameters=')[1]
  return DIRAC.S_OK()

def setCorsikaTemplate( optionValue ):
  global corsikaTemplate
  corsikaTemplate = optionValue
  return DIRAC.S_OK()

def setExecutable( optionValue ):
  global executable
  executable = optionValue
  return DIRAC.S_OK()

def setVersion( optionValue ):
  global version
  version = optionValue
  return DIRAC.S_OK()

def setMode( optionValue ):
  global mode
  mode = optionValue
  return DIRAC.S_OK()

def setStorageElement( optionValue ):
  global storage_element
  storage_element = optionValue
  return DIRAC.S_OK()

def sendOutputCorsika(stdid,line):
  logfilename = executable + '.log'
  f = open( logfilename,'a')
  f.write(line)
  f.write('\n')
  f.close()

def sendOutputSimTel(stdid,line):
  logfilename = 'simtel.log'
  f = open( logfilename,'a')
  f.write(line)
  f.write('\n')
  f.close()

def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

def main():

  from DIRAC.Core.Base import Script

  Script.registerSwitch( "p:", "run_number=", "Run Number", setRunNumber )
  Script.registerSwitch( "T:", "template=", "Template", setCorsikaTemplate )
  Script.registerSwitch( "E:", "executable=", "Executable", setExecutable )
  Script.registerSwitch( "V:", "version=", "Version", setVersion )
  Script.registerSwitch( "M:", "mode=", "Mode", setMode )
  Script.registerSwitch( "D:", "storage_element=", "Storage Element", setStorageElement )

  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  Script.parseCommandLine()
  global fcc  
  from CTADIRAC.Core.Workflow.Modules.CorsikaApp import CorsikaApp
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from DIRAC.Core.Utilities.Subprocess import systemCall
  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
  
  jobID = os.environ['JOBID']
  jobID = int( jobID )
  jobReport = JobReport( jobID )

  CorsikaSimtelPack = 'corsika_simhessarray/' + version + '/corsika_simhessarray'

  packs = [CorsikaSimtelPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        installSoftwareEnviron( package, workingArea() )
        packageTuple =  package.split('/')
        corsika_subdir = sharedArea() + '/' + packageTuple[0] + '/' + version  
        cmd = 'cp -u -r ' + corsika_subdir + '/* .'       
        os.system(cmd)
        continue
    if workingArea:
      if checkSoftwarePackage( package, workingArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Local Area:', package )
        continue
      if installSoftwarePackage( package, workingArea() )['OK']:
      ############## compile #############################
        if version == 'prod-2_21122012':
          cmdTuple = ['./build_all','prod2','qgs2']
        else:
          cmdTuple = ['./build_all','ultra','qgs2']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to compile')
          DIRAC.exit( -1 )
        continue

    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  

 ###########
  ## Checking MD coherence
  fc = FileCatalog('LcgFileCatalog')
  res = fc._getCatalogConfigDetails('DIRACFileCatalog')
  print 'DFC CatalogConfigDetails:',res
  res = fc._getCatalogConfigDetails('LcgFileCatalog')
  print 'LCG CatalogConfigDetails:',res
  
  fcc = FileCatalogClient()
  ############################
  #############
  # CLAUDIA: simtelConfigFile should be built from ???
  simtelConfigFilesPath = 'sim_telarray/multi' 
  simtelConfigFile = simtelConfigFilesPath + '/multi_cta-ultra5.cfg'  
  #simtelConfigFile = simtelConfigFilesPath + '/multi_cta-prod1s.cfg'                         
  createGlobalsFromConfigFiles('prodConfigFile', corsikaTemplate, simtelConfigFile)
  

  ######################Building corsika Directory Metadata #######################
  
  resultCreateCorsikaDirMD = createCorsikaFileSystAndMD()  
  if not resultCreateCorsikaDirMD['OK']:
    DIRAC.gLogger.error( 'Failed to create corsika Directory MD')
    jobReport.setApplicationStatus('Failed to create corsika Directory MD')
    DIRAC.gLogger.error('Metadata coherence problem, no corsikaFile produced')
    DIRAC.exit( -1 )
  else:
    print 'corsika Directory MD successfully created'
    
  ############ Producing Corsika File
  cs = CorsikaApp()
  cs.setSoftwarePackage(CorsikaSimtelPack)
  cs.csExe = executable
  cs.csArguments = ['--run-number',run_number,'--run','corsika',corsikaTemplate] 
  corsikaReturnCode = cs.execute()
  
  if corsikaReturnCode != 0:
    DIRAC.gLogger.error( 'Corsika Application: Failed')
    jobReport.setApplicationStatus('Corsika Application: Failed')
    DIRAC.exit( -1 )
###################### rename of corsika output file #######################
  rundir = 'run' + run_number
  filein = rundir + '/' + corsikaOutputFileName
  corsikaFileName = particle + '_' + 'run' + run_number +  '.corsika.gz'
  mv_cmd = 'mv ' + filein + ' ' + corsikaFileName
  if(os.system(mv_cmd)):
    DIRAC.exit( -1 )
########################  
  ### create corsika tar luisa ####################
  corsikaTarName = particle + '_' + 'run' + run_number +  '.corsika.tar.gz'
  filetar1 = rundir + '/'+'input'
  filetar2 = rundir + '/'+ 'DAT' + run_number + '.dbase'
  filetar3 = rundir + '/run' + str(int(run_number)) + '.log'
  cmdTuple = ['/bin/tar','zcf',corsikaTarName, filetar1,filetar2,filetar3]
  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  ret = systemCall( 0, cmdTuple, sendOutput)
  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute tar')
    DIRAC.exit( -1 )

### to be put at the end of the job ###########################
  corsikaOutFileLFN = "%s/%s/Data/%s" % (corsikaDirPath,particle,corsikaFileName)
  print 'corsikaoutfileLFN:',corsikaOutFileLFN
    
  # Put and Register Corsika File into SRM, DFC and LFC, in a coherent manner
  result = myAddFile(corsikaOutFileLFN, corsikaFileName, storage_element)
  print "result of myAddFile for corsikaFile: ", result
    
  # put and register corsikaTarFile:
  corsikaTarFileLFN = "%s/%s/Tar/%s" % (corsikaDirPath,particle,corsikaTarName)
  print 'corsikaTarFileLFN:',corsikaTarFileLFN
  
  result = myAddFile(corsikaTarFileLFN, corsikaTarName, storage_element)
  print "result of myAddFile for corsikatarFile: ", result

 ###### insert corsika File Level metadata ############################################
  corsikaFileMD={}
  corsikaFileMD['runNumber'] = int(run_number)
  corsikaFileMD['jobID'] = jobID
  corsikaFileMD['corsikaReturnCode'] = corsikaReturnCode

  result = fcc.setMetadata(corsikaOutFileLFN,corsikaFileMD)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']

#####  Exit now if only corsika simulation required
  if (mode == 'corsika_standalone'):
    DIRAC.exit()

############ Producing SimTel File
 ######################Building simtel Directory Metadata #######################
  
  resultCreateSimtelDirMD = createSimtelFileSystAndMD()  
  if not resultCreateSimtelDirMD['OK']:
    DIRAC.gLogger.error( 'Failed to create simtelArray Directory MD')
    jobReport.setApplicationStatus('Failed to create simtelArray Directory MD')
    DIRAC.gLogger.error('Metadata coherence problem, no simtelArray File produced')
    DIRAC.exit( -1 )
  else:
    print 'simtel Directory MD successfully created'
  

#### execute simtelarray ################
  fd = open('run_sim.sh', 'w' )
  fd.write( """#! /bin/sh                                                                                                                         
echo "go for sim_telarray"
. ./examples_common.sh
zcat %s | $SIM_TELARRAY_PATH/run_sim_%s""" % (corsikaFileName, simtelExecName))
  fd.close()

  os.system('chmod u+x run_sim.sh')

  cmdTuple = ['./run_sim.sh']
  ret = systemCall( 0, cmdTuple, sendOutputSimTel)
  simtelReturnCode, stdout, stderr = ret['Value']

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
    DIRAC.gLogger.error( 'run_sim.sh status is:', simtelReturnCode)
    DIRAC.exit( -1 )

## putAndRegister simtel data/log/histo Output File:
########### 0.0deg it's hard coded or is the offset?

  simtelFileName = particle + '_' + zen + '_' + az + '_' + 'run' + run_number + '.simtel.gz' 
  cmd = 'mv Data/sim_telarray/' + simtelExecName + '/0.0deg/Data/*.simtel.gz ' + simtelFileName 
  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutFileLFN = "%s/%s/Data/%s" % (simtelDirPath,particle,simtelFileName)

  simtelLogFileName = particle + '_' + zen + '_' + az + '_' + 'run' + run_number + '.log.gz' 
  cmd = 'mv Data/sim_telarray/' + simtelExecName + '/0.0deg/Log/*.log.gz ' + simtelLogFileName 
  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutLogFileLFN = "%s/%s/Log/%s" % (simtelDirPath,particle,simtelLogFileName)

  simtelHistFileName = particle + '_' + zen + '_' + az + '_' + 'run' + run_number + '.hdata.gz' 
  cmd = 'mv Data/sim_telarray/' + simtelExecName + '/0.0deg/Histograms/*.hdata.gz ' + simtelHistFileName 
  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutHistFileLFN = "%s/%s/Histograms/%s" % (simtelDirPath,particle,simtelHistFileName)
  
  DIRAC.gLogger.notice( 'Put and register simtel File in LFC and DFC:', simtelOutFileLFN)
  ret = myAddFile( simtelOutFileLFN, simtelFileName, storage_element )

  DIRAC.gLogger.notice( 'Put and register simtel Log File in LFC and DFC:', simtelOutLogFileLFN)
  ret = myAddFile( simtelOutLogFileLFN, simtelLogFileName, storage_element )

  DIRAC.gLogger.notice( 'Put and register simtel Histo File in LFC and DFC:', simtelOutHistFileLFN)
  ret = myAddFile( simtelOutHistFileLFN, simtelHistFileName, storage_element )

  
###### simtel File level metadata ############################################

  simtelFileMD={}
  simtelFileMD['runNumber'] = int(run_number)
  simtelFileMD['jobID'] = jobID
  simtelFileMD['simtelReturnCode'] = simtelReturnCode
  result = fcc.setMetadata(simtelOutFileLFN,simtelFileMD)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']

  result = fcc.addFileAncestors({simtelOutFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.setMetadata(simtelOutFileLFN,simtelFileMD)
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']
    
  DIRAC.exit()

def myAddFile(lfn, fullPath, diracSE):
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  from DIRAC.Core.Utilities.File import makeGuid, getSize
  from DIRAC.Interfaces.API.Dirac import Dirac

  dirac = Dirac()
  rm = ReplicaManager()
    
  # put and Register into LFC:
  res1 = dirac.addFile(lfn, fullPath, diracSE)
    
  if not res1['OK']:
    DIRAC.gLogger.error('Error during LFC putAndRegister call:', res1['Message'])
    DIRAC.exit( -1 )
     
 # registerFile into DFC
  size = getSize( fullPath )
  fileTuple = (lfn, fullPath, size, diracSE, None, None)
  res2 = rm.registerFile( fileTuple, 'DIRACFileCatalog' )

  if not res2['OK']:
    DIRAC.gLogger.error('Error during DFC registerFile call:', res2['Message'])
    DIRAC.gLogger.notice('Removing File from the LFC')
    res3 = dirac.removeFile(lfn)
    if not res3['OK']:
      DIRAC.gLogger.error('Failed to remove File from the LFC: to be done by hand', res3['Message'])
      DIRAC.exit( -1 )
    DIRAC.exit( -1 )
   
  return DIRAC.S_OK ('Put and register File in both Catalogs: Successful')

def createGlobalsFromConfigFiles(prodConfigFileName, corsikaConfigFileName, simtelConfigFileName):

  global prodName
  global zen 
  global az
  global particle 
  global energyInfo 
  global viewCone 
  global pathroot
  global nbShowers
  global simtelOffset
  global prodDirPath
  global corsikaDirPath
  global corsikaParticleDirPath
  global simtelDirPath
  global corsikaOutputFileName
  global simtelExecName
  global corsikaProdVersion
  global simtelProdVersion 

  # Getting MD Values from Config Files:
  prodKEYWORDS =  ['prodName','simtelExeName','pathroot']
  dictProdKW = fileToKWDict(prodConfigFileName,prodKEYWORDS)

  corsikaKEYWORDS = ['THETAP', 'PHIP', 'PRMPAR', 'ESLOPE' , 'ERANGE', 'VIEWCONE','NSHOW','TELFIL']
  dictCorsikaKW = fileToKWDict(corsikaConfigFileName,corsikaKEYWORDS)

  simtelKEYWORDS = ['env offset']

  # Formatting MD values retrieved in configFiles
  prodName = dictProdKW['prodName'][0]
  corsikaProdVersion = version + '_corsika'
  simtelProdVersion = version + '_simtel'
  zen = str(float(dictCorsikaKW['THETAP'][0]))
  az = str(float(dictCorsikaKW['PHIP'][0]))
  nbShowers = str(int(dictCorsikaKW['NSHOW'][0]))
  corsikaOutputFileName = dictCorsikaKW['TELFIL'][0]  
  simtelExecName = dictProdKW['simtelExeName'][0]
  
  #building simtelArray Offset
  dictSimtelKW={}
  simtelConfigFile = open(simtelConfigFileName, "r").readlines()
  for line in simtelConfigFile:
    lineSplitEqual = line.split('=')
    isAComment = '#' in lineSplitEqual[0].split()
    for word in lineSplitEqual:
      if (word in simtelKEYWORDS and not isAComment) :
        offset = lineSplitEqual[1].split()[0]
        #print 'offset=', offset
        dictSimtelKW[word] = offset
  #print 'dictSimtelKW:',dictSimtelKW
  simtelOffset = dictSimtelKW['env offset'] 
  #print 'simtelOffset',simtelOffset
  
  
  #building ParticleName
  dictParticleCode={}
  dictParticleCode['1'] = 'gamma'
  dictParticleCode['14'] = 'proton'
  dictParticleCode['3'] = 'electron'
  particleCode = dictCorsikaKW['PRMPAR'][0]
  particle = dictParticleCode[particleCode]

  #building energy info:
  eslope = dictCorsikaKW['ESLOPE'][0]
  eRange = dictCorsikaKW['ERANGE']
  emin = eRange[0]
  emax = eRange[1] 
  energyInfo = eslope + '_' + emin + '-' + emax
  
  #building viewCone
  viewConeRange = dictCorsikaKW['VIEWCONE'] 
  viewCone = str(float(viewConeRange[1]))
  
  pathroot = dictProdKW['pathroot'][0]
  #building full prod, corsika and simtel Directories path
  prodDirPath = "%s%s" % (pathroot,prodName)
  #print 'prodDirPath= ', prodDirPath
  corsikaPathTuple = [prodName,corsikaProdVersion]
  corsikaDirPath = "%s%s" % ( pathroot, '/'.join( corsikaPathTuple ) ) 
  #print 'corsikaDirPath=',corsikaDirPath 
  corsikaParticleDirPath = "%s/%s" % (corsikaDirPath,particle)
  #print 'corsikaParticleDirPath=',corsikaParticleDirPath  
  simtelDirPath = '%s/%s' % (corsikaDirPath,simtelProdVersion)
  #print 'simtelDirPath=', simtelDirPath

  
def fileToKWDict (fileName, keywordsList):    
  #print 'parsing %s...' % fileName
  dict={}
  configFile = open(fileName, "r").readlines()
  for line in configFile:
    if (len(line.split())>0):
      for word in line.split():
        if line.split()[0] is not '*' and word in keywordsList:
          lineSplit = line.split()
          lenLineSplit = len(lineSplit)
          value = lineSplit[1:lenLineSplit]
          dict[word] = value
  return dict


def createIndexes(indexesTypesDict):
  #CLAUDIA ToBeDone: only if they don't already exist: waiting for Release update
  for mdField in indexesTypesDict.keys():
    mdFieldType = indexesTypesDict[mdField]
    #print 'mdField,mdFieldType: %s,%s' % (mdField,mdFieldType)
    result = fcc.addMetadataField(mdField,mdFieldType)
    print 'result addMetadataField %s: %s' % (mdField,result)

def createDirAndInsertMD(dirPath, requiredDirMD):
  # if directory already exist, verify if metadata already exist.  If not, create directory and associated MD 
  print 'starting... createDirAndInsertMD(%s)' % dirPath
  dirExists = fcc.isDirectory(dirPath)['Value']['Successful'][dirPath]
  print 'dirExist result',dirExists
  if (dirExists):
    print 'Directory already exists'      
    dirMDV = fcc.getDirectoryMetadata(dirPath)[ 'Value' ]
 
    dirMDVK = fcc.getDirectoryMetadata(dirPath)[ 'Value' ].keys()
    #print 'dirPath=', dirPath
    #print 'dirMD=', dirMDV
    #print 'dirMDKeys=', dirMDVK
    
    dirMDVV = fcc.getDirectoryMetadata(dirPath)[ 'Value' ].values()
    #print 'dirMDValues=', dirMDVV
    
    
    #verify if all the MDvalues already exist, if not, insert their value
    mdToAdd={}
    for mdkey,mdvalue in requiredDirMD.iteritems():
      if mdkey not in dirMDVK:
        #print 'key %s does not exist, insert value' % mdkey
        mdToAdd[mdkey] = requiredDirMD[mdkey]     
      else:
        #print 'key,value=%s,%s' % (mdkey,mdvalue)
        #print  'value=%s, required value=%s' % (dirMDV[mdkey],mdvalue)
        test = (str(dirMDV[mdkey]) == str(mdvalue))
        #print 'dirMDV[mdkey] == mdvalue?' , test
        
        if (test == False):
      	  print 'metadata key exist, but values are not coherent: actual value=%s, required value=%s' % (dirMDV[mdkey],mdvalue)
      	  return DIRAC.S_ERROR ('MD Error: Problem during Dir Metadata verification, values are not coherent at least for %s key' % mdkey)
        else:
          print 'metadata key exist, and values are coherent'
      	
 
    if len(mdToAdd) > 0:
      result = fcc.setMetadata(dirPath,mdToAdd)
      print "%d metadata added" % len(mdToAdd)
    else:
      print 'no MD needed to be added'
  else:
    print 'New directory, creating path '
    res = fcc.createDirectory(dirPath)
    print "createDir res:", res
    # insert Directory level MD:
    result = fcc.setMetadata(dirPath,requiredDirMD)
    print 'result setMetadataDir:',result
  return DIRAC.S_OK
  
  
def createProdDirIndexes():
  # before creating indexes, it would be fine to know what are those that already exist in the DB
  # Creating INDEXES in DFC DB
  prodDirMDFields={}
  prodDirMDFields['lastRunNumber'] = 'int'
  createIndexes(prodDirMDFields)    
  
def createCorsikaFileSystAndMD():
  # before creating indexes, it would be fine to know what are those that already exist in the DB
  # Creating INDEXES in DFC DB
  corsikaDirMDFields={}
  corsikaDirMDFields['prodName'] = 'VARCHAR(128)'
  corsikaDirMDFields['zen'] = 'float'
  corsikaDirMDFields['az'] = 'float'
  corsikaDirMDFields['particle'] = 'VARCHAR(128)'  
  corsikaDirMDFields['energyInfo'] = 'VARCHAR(128)'
  corsikaDirMDFields['viewCone'] = 'float'
  corsikaDirMDFields['corsikaProdVersion'] = 'VARCHAR(128)'
  corsikaDirMDFields['nbShowers'] = 'int'
  createIndexes(corsikaDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  corsikaDirMD={}
  corsikaDirMD['prodName'] = prodName
  corsikaDirMD['zen'] = zen
  corsikaDirMD['az'] = az
  #corsikaDirMD['particle'] = particle  
  corsikaDirMD['energyInfo'] = energyInfo
  corsikaDirMD['viewCone'] = viewCone
  corsikaDirMD['corsikaProdVersion'] = corsikaProdVersion
  corsikaDirMD['nbShowers'] = nbShowers
    
  res = createDirAndInsertMD(corsikaDirPath, corsikaDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Directory MD ')
    
  corsikaParticleDirMD={}
  corsikaParticleDirMD['particle'] = particle    
  res = createDirAndInsertMD(corsikaParticleDirPath, corsikaParticleDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Particle Directory MD ')
        
  return DIRAC.S_OK ('Corsika Directory MD successfully created')

 
def createSimtelFileSystAndMD():
  # Creating INDEXES in DFC DB
  simtelDirMDFields={}
  simtelDirMDFields['simtelArrayProdVersion'] = 'VARCHAR(128)'
  simtelDirMDFields['offset'] = 'float'
  createIndexes(simtelDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  simtelDirMD={}
  simtelDirMD['simtelArrayProdVersion'] = simtelProdVersion
  simtelOffsetCorr = simtelOffset[1:-1]
  #print 'simtelOffsetCorr=',simtelOffsetCorr
  simtelDirMD['offset'] = float(simtelOffsetCorr)

  res = createDirAndInsertMD(simtelDirPath, simtelDirMD)
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('MD Error: Problem creating Simtel Directory MD ')
    
  return DIRAC.S_OK ('Simtel Directory MD successfully created')



if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



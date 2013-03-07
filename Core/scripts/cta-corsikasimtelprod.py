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
  global fcc, storage_element
  
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
  fcL = FileCatalog('LcgFileCatalog')
  
  from DIRAC.Interfaces.API.Dirac import Dirac
  dirac = Dirac()
  
  #############
  simtelConfigFilesPath = 'sim_telarray/multi'
  simtelConfigFile = simtelConfigFilesPath + '/multi_cta-ultra5.cfg'  
  #simtelConfigFile = simtelConfigFilesPath + '/multi_cta-prod1s.cfg'                         
  createGlobalsFromConfigFiles('prodConfigFile', corsikaTemplate, simtelConfigFile)
  
  ######################Building prod Directory Metadata #######################
  resultCreateProdDirMD = createProdFileSystAndMD()  
  if not resultCreateProdDirMD['OK']:
    DIRAC.gLogger.error( 'Failed to create prod Directory MD')
    jobReport.setApplicationStatus('Failed to create prod Directory MD')
    DIRAC.gLogger.error('Metadata coherence problem, no file produced')
    DIRAC.exit( -1 )
  else:
    print 'prod Directory MD successfully created'

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
  corsikaFileName = particle + '_' + thetaP + '_' + phiP + '_alt' + obslev + '_' + 'run' + run_number +  '.corsika.gz'
  mv_cmd = 'mv ' + filein + ' ' + corsikaFileName
  if(os.system(mv_cmd)):
    DIRAC.exit( -1 )
########################  

########################  
## files spread in 1000-runs subDirectories
  runNum = int(run_number)
  subRunNumber = '%03d'%runNum
  runNumModMille = runNum%1000
  runNumTrunc = (runNum - runNumModMille)/1000
  runNumSeriesDir = '%03dxxx'%runNumTrunc
  print 'runNumSeriesDir=',runNumSeriesDir
  
  ### create corsika tar luisa ####################
  corsikaTarName = particle + '_' + thetaP + '_' + phiP + '_alt' + obslev + '_' + 'run' + run_number +  '.corsika.tar.gz'
  filetar1 = rundir + '/'+'input'
  filetar2 = rundir + '/'+ 'DAT' + run_number + '.dbase'
  filetar3 = rundir + '/run' + str(int(run_number)) + '.log'
  cmdTuple = ['/bin/tar','zcf',corsikaTarName, filetar1,filetar2,filetar3]
  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  ret = systemCall( 0, cmdTuple, sendOutput)
  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute tar')
    DIRAC.exit( -1 )

######################################################   
  corsikaOutFileDir = "%s/%s/Data/%s" % (corsikaDirPath,particle,runNumSeriesDir)
  corsikaOutFileLFN = "%s/%s" % (corsikaOutFileDir,corsikaFileName)
  corsikaRunNumberSeriesDirExist = fcc.isDirectory(corsikaOutFileDir)['Value']['Successful'][corsikaOutFileDir]
  newCorsikaRunNumberSeriesDir = (corsikaRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

#### create a file to DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK ################
  f = open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
  f.close()

  DIRAC.gLogger.notice( 'Put and register corsika File in LFC and DFC:', corsikaOutFileLFN)
  ret = dirac.addFile(corsikaOutFileLFN, corsikaFileName, storage_element)  
  
  if ret['OK']:
    if len(ret['Value']['Successful'][corsikaOutFileLFN].keys())!=2:
      DIRAC.gLogger.error('Error during addFile: put or register missing')
      jobReport.setApplicationStatus('OutputData Upload Error')
      ############# restablishing catalogs coherence ##########################
      DIRAC.gLogger.notice('Try to restablish Catalogs coherence')
      res = fcc.getReplicas(corsikaOutFileLFN)
      if len(res['Value']['Successful'])!=0:
        DIRAC.gLogger.notice('Found in DFC',corsikaOutFileLFN )
        res = fcc.removeFile(corsikaOutFileLFN)
        print 'removing res', res
      DIRAC.exit( -1 )
  else:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
    
  # put and register corsikaTarFile:
  corsikaTarFileDir = "%s/%s/Log/%s" % (corsikaDirPath,particle,runNumSeriesDir)
  corsikaTarFileLFN = "%s/%s" % (corsikaTarFileDir,corsikaTarName)

##### If storage element is IN2P3-tape save simtel file on disk ###############  
  if storage_element == 'CC-IN2P3-Tape':
    storage_element = 'CC-IN2P3-Disk'

  DIRAC.gLogger.notice( 'Put and register corsikaTar File in LFC and DFC:', corsikaTarFileLFN)
  ret = dirac.addFile(corsikaTarFileLFN, corsikaTarName, storage_element)
  
  if ret['OK']:
    if len(ret['Value']['Successful'][corsikaTarFileLFN].keys())!=2:
      DIRAC.gLogger.error('Error during addFile: put or register missing')
      jobReport.setApplicationStatus('OutputData Upload Error')
      ############# restablishing catalogs coherence ##########################
      DIRAC.gLogger.notice('Try to restablish Catalogs coherence')
      res = fcc.getReplicas(corsikaTarFileLFN)
      if len(res['Value']['Successful'])!=0:
        DIRAC.gLogger.notice('Found in DFC',corsikaTarFileLFN )
        res = fcc.removeFile(corsikaTarFileLFN)
        print 'removing res', res
      DIRAC.exit( -1 )
  else:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
      
  if newCorsikaRunNumberSeriesDir:
    insertRunFileSeriesMD(corsikaOutFileDir,runNumTrunc)
    insertRunFileSeriesMD(corsikaTarFileDir,runNumTrunc)

 ###### insert corsika File Level metadata ############################################
  corsikaFileMD={}
  corsikaFileMD['runNumber'] = int(run_number)
  corsikaFileMD['jobID'] = jobID
  corsikaFileMD['corsikaReturnCode'] = corsikaReturnCode
  corsikaFileMD['nbShowers'] = nbShowers

  result = fcc.setMetadata(corsikaOutFileLFN,corsikaFileMD)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']

  result = fcc.setMetadata(corsikaTarFileLFN,corsikaFileMD)
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
export CORSIKA_IO_BUFFER=800MB
zcat %s | $SIM_TELARRAY_PATH/run_sim_%s""" % (corsikaFileName, simtelExecName))
  fd.close()

  os.system('chmod u+x run_sim.sh')

  cmdTuple = ['./run_sim.sh']
  ret = systemCall( 0, cmdTuple, sendOutputSimTel)
  simtelReturnCode, stdout, stderr = ret['Value']

  if(os.system('grep Broken simtel.log')):
    print 'not broken'
  else:
    print 'broken'
    
    # Tag corsika File if Broken Pipe
    corsikaTagMD={}
    corsikaTagMD['CorsikaToReprocess'] = 'CorsikaToReprocess'
    result = fcc.setMetadata(corsikaOutFileLFN,corsikaTagMD)
    print "result setMetadata=",result
    if not result['OK']:
      print 'ResultSetMetadata:',result['Message']
  
    jobReport.setApplicationStatus('Broken pipe')
    DIRAC.exit( -1 )

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
    DIRAC.gLogger.error( 'run_sim.sh status is:', simtelReturnCode)
    DIRAC.exit( -1 )

## putAndRegister simtel data/log/histo Output File:
########### 0.0deg it's hard coded or is the offset?

  simtelFileName = particle + '_' + thetaP + '_' + phiP + '_alt' + obslev + '_' + 'run' + run_number + '.simtel.gz'
  cmd = 'mv Data/sim_telarray/' + simtelExecName + '/0.0deg/Data/*.simtel.gz ' + simtelFileName
  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutFileDir = "%s/Data/%s" % (simtelDirPath,runNumSeriesDir)
  simtelOutFileLFN = "%s/%s" % (simtelOutFileDir,simtelFileName)
  simtelRunNumberSeriesDirExist = fcc.isDirectory(simtelOutFileDir)['Value']['Successful'][simtelOutFileDir]
  newSimtelRunFileSeriesDir = (simtelRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

  simtelLogFileName = particle + '_' + thetaP + '_' + phiP + '_alt' + obslev + '_' + 'run' + run_number + '.log.gz'
  cmd = 'mv Data/sim_telarray/' + simtelExecName + '/0.0deg/Log/*.log.gz ' + simtelLogFileName
  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutLogFileDir = "%s/Log/%s" % (simtelDirPath,runNumSeriesDir)
  simtelOutLogFileLFN = "%s/%s" % (simtelOutLogFileDir,simtelLogFileName)

  simtelHistFileName = particle + '_' + thetaP + '_' + phiP + '_alt' + obslev + '_' + 'run' + run_number + '.hdata.gz'
  cmd = 'mv Data/sim_telarray/' + simtelExecName + '/0.0deg/Histograms/*.hdata.gz ' + simtelHistFileName
  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutHistFileDir = "%s/Histograms/%s" % (simtelDirPath,runNumSeriesDir)
  simtelOutHistFileLFN = "%s/%s" % (simtelOutHistFileDir,simtelHistFileName)
  
################################################  
  DIRAC.gLogger.notice( 'Put and register simtel File in LFC and DFC:', simtelOutFileLFN)
  ret = dirac.addFile( simtelOutFileLFN, simtelFileName, storage_element )   

  if ret['OK']:
    if len(ret['Value']['Successful'][simtelOutFileLFN].keys())!=2:
      DIRAC.gLogger.error('Error during addFile: put or register missing')
      jobReport.setApplicationStatus('OutputData Upload Error')      
      ############# restablishing catalogs coherence ##########################
      DIRAC.gLogger.notice('Try to restablish Catalogs coherence')
      res = fcc.getReplicas(simtelOutFileLFN)
      if len(res['Value']['Successful'])!=0:
        DIRAC.gLogger.notice('Found in DFC',simtelOutFileLFN )
        res = fcc.removeFile(simtelOutFileLFN)
        print 'removing res', res
      DIRAC.exit( -1 )
  else:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )

  DIRAC.gLogger.notice( 'Put and register simtel Log File in LFC and DFC:', simtelOutLogFileLFN)
  ret = dirac.addFile( simtelOutLogFileLFN, simtelLogFileName, storage_element )

#  if ret['OK']:
################################################    
  res = fcc.getReplicas(simtelOutLogFileLFN)  
  ndfc = len(res['Value']['Successful'])
  if ndfc!=0:
    DIRAC.gLogger.notice('Found in DFC',simtelOutLogFileLFN )
  res = fcL.getReplicas(simtelOutLogFileLFN)
  nlfc = len(res['Value']['Successful'])
  if nlfc!=0:
    DIRAC.gLogger.notice('Found in LFC',simtelOutLogFileLFN )
  if ndfc>nlfc:
    DIRAC.gLogger.error('Catalogs are not coherent: removing file from DFC',simtelOutLogFileLFN)
    res = fcc.removeFile(simtelOutLogFileLFN)
  elif ndfc<nlfc:
    DIRAC.gLogger.error('Catalogs are not coherent: removing file from LFC',simtelOutLogFileLFN)
    res = fcL.removeFile(simtelOutLogFileLFN)
     
  if not ret['OK']:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
    

  DIRAC.gLogger.notice( 'Put and register simtel Histo File in LFC and DFC:', simtelOutHistFileLFN)
  ret = dirac.addFile( simtelOutHistFileLFN, simtelHistFileName, storage_element )

  if ret['OK']:
    if len(ret['Value']['Successful'][simtelOutHistFileLFN].keys())!=2:
      DIRAC.gLogger.error('Error during addFile: put or register missing')
      jobReport.setApplicationStatus('OutputData Upload Error')
      ############# restablishing catalogs coherence ##########################
      DIRAC.gLogger.notice('Try to restablish Catalogs coherence')
      res = fcc.getReplicas(simtelOutHistFileLFN)
      if len(res['Value']['Successful'])!=0:
        DIRAC.gLogger.notice('Found in DFC',simtelOutHistFileLFN)
        res = fcc.removeFile(simtelOutHistFileLFN)
        print 'removing res', res
      DIRAC.exit( -1 )
  else:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
    
  if newSimtelRunFileSeriesDir:
    insertRunFileSeriesMD(simtelOutFileDir,runNumTrunc)
    insertRunFileSeriesMD(simtelOutLogFileDir,runNumTrunc)
    insertRunFileSeriesMD(simtelOutHistFileDir,runNumTrunc)
    
###### simtel File level metadata ############################################

  simtelFileMD={}
  simtelFileMD['runNumber'] = int(run_number)
  simtelFileMD['jobID'] = jobID
  simtelFileMD['simtelReturnCode'] = simtelReturnCode
  
  result = fcc.setMetadata(simtelOutFileLFN,simtelFileMD)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']

  result = fcc.setMetadata(simtelOutLogFileLFN,simtelFileMD)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']

  result = fcc.setMetadata(simtelOutHistFileLFN,simtelFileMD)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']

  result = fcc.addFileAncestors({simtelOutFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.addFileAncestors({simtelOutLogFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.addFileAncestors({simtelOutHistFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.setMetadata(simtelOutFileLFN,simtelFileMD)
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']
    
  DIRAC.exit()

def createGlobalsFromConfigFiles(prodConfigFileName, corsikaConfigFileName, simtelConfigFileName):

  global prodName
  global thetaP
  global phiP
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
  global obslev

  # Getting MD Values from Config Files:
  prodKEYWORDS =  ['prodName','simtelExeName','pathroot']
  dictProdKW = fileToKWDict(prodConfigFileName,prodKEYWORDS)

  corsikaKEYWORDS = ['THETAP', 'PHIP', 'PRMPAR', 'ESLOPE' , 'ERANGE', 'VIEWCONE','NSHOW','TELFIL','OBSLEV']
  dictCorsikaKW = fileToKWDict(corsikaConfigFileName,corsikaKEYWORDS)

  simtelKEYWORDS = ['env offset']

  # Formatting MD values retrieved in configFiles
  prodName = dictProdKW['prodName'][0]
  corsikaProdVersion = version + '_corsika'
  simtelProdVersion = version + '_simtel'
  thetaP = str(float(dictCorsikaKW['THETAP'][0]))
  phiP = str(float(dictCorsikaKW['PHIP'][0]))
  obslev = str(float(dictCorsikaKW['OBSLEV'][0])/100.)#why on earth is this in cm....
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
        dictSimtelKW[word] = offset
  simtelOffset = dictSimtelKW['env offset']
  
  #building viewCone
  viewConeRange = dictCorsikaKW['VIEWCONE']
  viewCone = str(float(viewConeRange[1]))
    
  #building ParticleName
  dictParticleCode={}
  dictParticleCode['1'] = 'gamma'
  dictParticleCode['14'] = 'proton'
  dictParticleCode['3'] = 'electron'
  dictParticleCode['402'] = 'helium'
  dictParticleCode['1407'] = 'nitrogen'
  dictParticleCode['2814'] = 'silicon'
  dictParticleCode['5626'] = 'iron'
  particleCode = dictCorsikaKW['PRMPAR'][0]
  particle = dictParticleCode[particleCode]
  if viewCone=='0.0':
    particle+="_ptsrc"

  #building energy info:
  eslope = dictCorsikaKW['ESLOPE'][0]
  eRange = dictCorsikaKW['ERANGE']
  emin = eRange[0]
  emax = eRange[1]
  energyInfo = eslope + '_' + emin + '-' + emax
  
  pathroot = dictProdKW['pathroot'][0]
  #building full prod, corsika and simtel Directories path
  prodDirPath = "%s%s" % (pathroot,prodName)
  corsikaPathTuple = [prodName,corsikaProdVersion]
  corsikaDirPath = "%s%s" % ( pathroot, '/'.join( corsikaPathTuple ) )
  corsikaParticleDirPath = "%s/%s" % (corsikaDirPath,particle)
  simtelDirPath = '%s/%s' % (corsikaParticleDirPath,simtelProdVersion)
  
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
    result = fcc.addMetadataField(mdField,mdFieldType)
    print 'result addMetadataField %s: %s' % (mdField,result)

def createDirAndInsertMD(dirPath, requiredDirMD):
  # if directory already exist, verify if metadata already exist.  If not, create directory and associated MD
  print 'starting... createDirAndInsertMD(%s)' % dirPath
########## ricardo ##########################################
  dirExists = fcc.isDirectory(dirPath)
  if not dirExists['OK']:
    print dirExists['Message']
    return dirExists
  dirExists = dirExists['Value']['Successful'][dirPath]
############################################################
  print 'dirExist result',dirExists
  if (dirExists):
    print 'Directory already exists'
################# ricardo #################################
    dirMD = fcc.getDirectoryMetadata(dirPath)
    if not dirMD['OK']:
      print dirMD['Message']
      return dirMD
    dirMD = dirMD[ 'Value' ]

    #verify if all the MDvalues already exist, if not, insert their value
    mdToAdd={}
    for mdkey,mdvalue in requiredDirMD.iteritems():
      if mdkey not in dirMD:
        mdToAdd[mdkey] = requiredDirMD[mdkey]     
      else:
        test = (str(dirMD[mdkey]) == str(mdvalue))

        if (test == False):
      	  print 'metadata key exist, but values are not coherent: actual value=%s, required value=%s' % (dirMD[mdkey],mdvalue)
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
  

def insertRunFileSeriesMD(runNumSeriesPath,runNumSeries):
  runNumSeriesDirMD={}
  runNumSeriesDirMD['runNumSeries'] = runNumSeries * 1000
  fcc.setMetadata(runNumSeriesPath,runNumSeriesDirMD)
  
def createProdDirIndexes():
  # before creating indexes, it would be fine to know what are those that already exist in the DB
  # Creating INDEXES in DFC DB
  prodDirMDFields={}
  prodDirMDFields['lastRunNumber'] = 'int'
  createIndexes(prodDirMDFields)    
  
def createProdFileSystAndMD():
  # before creating indexes, it would be fine to know what are those that already exist in the DB
  # Creating INDEXES in DFC DB
  prodDirMDFields={}
  prodDirMDFields['prodName'] = 'VARCHAR(128)'
  createIndexes(prodDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  prodDirMD={}
  prodDirMD['prodName'] = prodName
    
  res = createDirAndInsertMD(prodDirPath, prodDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Prod Directory MD ')
        
  return DIRAC.S_OK ('Prod Directory MD successfully created')
  
def createCorsikaFileSystAndMD():
  # before creating indexes, it would be fine to know what are those that already exist in the DB
  # Creating INDEXES in DFC DB
  corsikaDirMDFields={}
  corsikaDirMDFields['prodName'] = 'VARCHAR(128)'
  corsikaDirMDFields['thetaP'] = 'float'
  corsikaDirMDFields['phiP'] = 'float'
  corsikaDirMDFields['altitude'] = 'float'
  corsikaDirMDFields['particle'] = 'VARCHAR(128)'  
  corsikaDirMDFields['energyInfo'] = 'VARCHAR(128)'
  corsikaDirMDFields['viewCone'] = 'float'
  corsikaDirMDFields['corsikaProdVersion'] = 'VARCHAR(128)'
  corsikaDirMDFields['nbShowers'] = 'int'  
  corsikaDirMDFields['outputType'] = 'VARCHAR(128)'
  corsikaDirMDFields['runNumSeries'] = 'int'  
  
  createIndexes(corsikaDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  corsikaDirMD={}
  corsikaDirMD['thetaP'] = thetaP
  corsikaDirMD['phiP'] = phiP
  corsikaDirMD['altitude'] = obslev
  corsikaDirMD['energyInfo'] = energyInfo
  corsikaDirMD['corsikaProdVersion'] = corsikaProdVersion

  res = createDirAndInsertMD(corsikaDirPath, corsikaDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Directory MD ')
    
  corsikaParticleDirMD={}
  corsikaParticleDirMD['particle'] = particle
  corsikaParticleDirMD['viewCone'] = viewCone
   
  res = createDirAndInsertMD(corsikaParticleDirPath, corsikaParticleDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Particle Directory MD ')
    
  corsikaParticleDataDirPath = '%s/%s' % (corsikaParticleDirPath,'Data')  
  corsikaParticleDataDirMD={}
  corsikaParticleDataDirMD['outputType'] = 'Data'
  res = createDirAndInsertMD(corsikaParticleDataDirPath, corsikaParticleDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Particle Data Directory MD ')

  corsikaParticleLogDirPath = '%s/%s' % (corsikaParticleDirPath,'Log')  
  corsikaParticleLogDirMD={}
  corsikaParticleLogDirMD['outputType'] = 'Log'
  res = createDirAndInsertMD(corsikaParticleLogDirPath, corsikaParticleLogDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Particle Log Directory MD ')
    
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
  simtelDirMD['offset'] = float(simtelOffsetCorr)

  res = createDirAndInsertMD(simtelDirPath, simtelDirMD)
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('MD Error: Problem creating Simtel Directory MD ')
    
  simtelDataDirPath = '%s/%s' % (simtelDirPath,'Data')
  simtelDataDirMD={}
  simtelDataDirMD['outputType'] = 'Data'
  res = createDirAndInsertMD(simtelDataDirPath, simtelDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Data Directory MD ')

  simtelLogDirPath = '%s/%s' % (simtelDirPath,'Log')
  simtelLogDirMD={}
  simtelLogDirMD['outputType'] = 'Log'
  res = createDirAndInsertMD(simtelLogDirPath, simtelLogDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Log Directory MD ')

  simtelHistoDirPath = '%s/%s' % (simtelDirPath,'Histograms')
  simtelHistoDirMD={}
  simtelHistoDirMD['outputType'] = 'Histo'
  res = createDirAndInsertMD(simtelHistoDirPath, simtelHistoDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Histo Directory MD ')
    
  return DIRAC.S_OK ('Simtel Directory MD successfully created')


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

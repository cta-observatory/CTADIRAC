#!/usr/bin/env python
import DIRAC
import os


def setInputFile ( optionValue ):
  global corsikaFileLFN
  corsikaFileLFN = optionValue.split('ParametricInputData=LFN:')[1]
  return DIRAC.S_OK()

def setExecutable( optionValue ):
  global simtelExecName
  simtelExecName = optionValue
  return DIRAC.S_OK()

def setConfig( optionValue ):
  global simtelConfig
  simtelConfig = optionValue
  return DIRAC.S_OK()

def setVersion( optionValue ):
  global version
  version = optionValue
  return DIRAC.S_OK()

def setStorageElement( optionValue ):
  global storage_element
  storage_element = optionValue
  return DIRAC.S_OK()

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

  Script.registerSwitch( "p:", "inputfile=", "Input File", setInputFile )
  Script.registerSwitch( "E:", "simtelExecName=", "SimtelExecName", setExecutable )
  Script.registerSwitch( "S:", "simtelConfig=", "SimtelConfig", setConfig )
  Script.registerSwitch( "V:", "version=", "Version", setVersion )
  Script.registerSwitch( "D:", "storage_element=", "Storage Element", setStorageElement )

  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  Script.parseCommandLine()
  global fcc, fcL, storage_element

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
  ############################

########## 
  # changed!!!!!
  #simtelConfigFilesPath = 'sim_telarray/multi'
  #simtelConfigFile = simtelConfigFilesPath + '/multi_cta-ultra5.cfg'                         
  simtelConfigFilesPath = 'CODE'
  simtelConfigFile = os.path.join(simtelConfigFilesPath,'multi_' + simtelConfig + '.cfg')
  createGlobalsFromConfigFiles(simtelConfigFile)

  #######################  
## files spread in 1000-runs subDirectories

  corsikaFileName = os.path.basename(corsikaFileLFN)
  run_number = corsikaFileName.split('run')[1].split('.corsika.gz')[0] # run001412.corsika.gz

  runNum = int(run_number)
  subRunNumber = '%03d'%runNum
  runNumModMille = runNum%1000
  runNumTrunc = (runNum - runNumModMille)/1000
  runNumSeriesDir = '%03dxxx'%runNumTrunc
  print 'runNumSeriesDir=',runNumSeriesDir
  
  f = open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
  f.close()

##### If storage element is IN2P3-tape save simtel file on disk ###############  
  if storage_element == 'CC-IN2P3-Tape':
    storage_element = 'CC-IN2P3-Disk'

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
export SVNPROD2=$PWD
source $SVNPROD2/CODE/prod2_simtel_config.sh build_%s
export CORSIKA_IO_BUFFER=800MB
zcat %s | $SVNPROD2/CODE/run_sim_prod2_generic %s""" % (simtelExecName, corsikaFileName, simtelConfig))
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
    result = fcc.setMetadata(corsikaFileLFN,corsikaTagMD)
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

##############################################

  simtelFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.simtel.gz'
  cmd = 'mv ' + os.path.join('Data/sim_telarray',simtelCfg,simtelOffset+'deg','Data/*.simtel.gz') + ' ' + simtelFileName
  if(os.system(cmd)):
    DIRAC.exit( -1 )

  simtelOutFileDir = os.path.join(simtelDirPath,'Data',runNumSeriesDir)
  simtelOutFileLFN = os.path.join(simtelOutFileDir,simtelFileName)
  simtelRunNumberSeriesDirExist = fcc.isDirectory(simtelOutFileDir)['Value']['Successful'][simtelOutFileDir]
  newSimtelRunFileSeriesDir = (simtelRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

  simtelLogFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.log.gz'
  cmd = 'mv ' + os.path.join('Data/sim_telarray',simtelCfg,simtelOffset+'deg','Log/*.log.gz') + ' ' + simtelLogFileName

  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutLogFileDir = os.path.join(simtelDirPath,'Log',runNumSeriesDir)
  simtelOutLogFileLFN = os.path.join(simtelOutLogFileDir,simtelLogFileName)

  simtelHistFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.hdata.gz'
  cmd = 'mv ' + os.path.join('Data/sim_telarray',simtelCfg,simtelOffset+'deg','Histograms/*.hdata.gz') + ' ' + simtelHistFileName

  if(os.system(cmd)):
    DIRAC.exit( -1 )
  simtelOutHistFileDir = os.path.join(simtelDirPath,'Histograms',runNumSeriesDir)
  simtelOutHistFileLFN = os.path.join(simtelOutHistFileDir,simtelHistFileName)

################################################  
  DIRAC.gLogger.notice( 'Put and register simtel File in LFC and DFC:', simtelOutFileLFN)
  ret = dirac.addFile( simtelOutFileLFN, simtelFileName, storage_element )   

  res = CheckCatalogCoherence(simtelOutFileLFN)
  if res != DIRAC.S_OK:
    DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
     
  if not ret['OK']:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
######################################################################

  DIRAC.gLogger.notice( 'Put and register simtel Log File in LFC and DFC:', simtelOutLogFileLFN)
  ret = dirac.addFile( simtelOutLogFileLFN, simtelLogFileName, storage_element )

  res = CheckCatalogCoherence(simtelOutLogFileLFN)
  if res != DIRAC.S_OK:
    DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
     
  if not ret['OK']:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
######################################################################

  DIRAC.gLogger.notice( 'Put and register simtel Histo File in LFC and DFC:', simtelOutHistFileLFN)
  ret = dirac.addFile( simtelOutHistFileLFN, simtelHistFileName, storage_element )

  res = CheckCatalogCoherence(simtelOutHistFileLFN)
  if res != DIRAC.S_OK:
    DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
     
  if not ret['OK']:
    DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
    jobReport.setApplicationStatus('OutputData Upload Error')
    DIRAC.exit( -1 )
######################################################################
    
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

  result = fcc.addFileAncestors({simtelOutFileLFN:{'Ancestors': [ corsikaFileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.addFileAncestors({simtelOutLogFileLFN:{'Ancestors': [ corsikaFileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.addFileAncestors({simtelOutHistFileLFN:{'Ancestors': [ corsikaFileLFN ] }})
  print 'result addFileAncestor:', result


  result = fcc.setMetadata(simtelOutFileLFN,simtelFileMD)
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']
    
  DIRAC.exit()

def CheckCatalogCoherence(fileLFN):
####Checking and restablishing catalog coherence #####################  
  res = fcc.getReplicas(fileLFN)  
  ndfc = len(res['Value']['Successful'])
  if ndfc!=0:
    DIRAC.gLogger.notice('Found in DFC:',fileLFN)
  res = fcL.getReplicas(fileLFN)
  nlfc = len(res['Value']['Successful'])
  if nlfc!=0:
    DIRAC.gLogger.notice('Found in LFC:',fileLFN)
  if ndfc>nlfc:
    DIRAC.gLogger.error('Catalogs are not coherent: removing file from DFC',fileLFN)
    res = fcc.removeFile(fileLFN)
    return DIRAC.S_ERROR
  elif ndfc<nlfc:
    DIRAC.gLogger.error('Catalogs are not coherent: removing file from LFC',fileLFN)
    res = fcL.removeFile(fileLFN)
    return DIRAC.S_ERROR
  elif (ndfc==0 and nlfc==0):
   DIRAC.gLogger.error('File not found in DFC and LFC:',fileLFN)
   return DIRAC.S_ERROR
    
  return DIRAC.S_OK

def createGlobalsFromConfigFiles(simtelConfigFileName):

  global prodName
  global thetaP
  global phiP
  global particle
  global energyInfo
  global viewCone
  global pathroot
  global simtelOffset
  global simtelCfg
  global corsikaDirPath
  global corsikaParticleDirPath
  global simtelDirPath
  global corsikaProdVersion
  global simtelProdVersion
  global obslev

  # Getting MD Values from Config Files:
  prodKEYWORDS =  ['prodName','simtelExeName','pathroot']

########################################################################
  corsikaDirPath = os.path.dirname(corsikaFileLFN)
  corsikaDirPathMD = fcc.getDirectoryMetadata(corsikaDirPath)

  if not corsikaDirPathMD['OK']:
    print corsikaDirPathMD['Message']
    return corsikaDirPathMD

  corsikaDirPathMD = corsikaDirPathMD[ 'Value' ]
  thetaP = corsikaDirPathMD['thetaP']
  phiP = corsikaDirPathMD['phiP']
  particle = corsikaDirPathMD['particle']
  obslev = corsikaDirPathMD['altitude']
#########################################################################
  
  simtelKEYWORDS = ['env offset','cfg','extra_defs']

  # Formatting MD values retrieved in configFiles
  corsikaProdVersion = version + '_corsika'
  simtelProdVersion = version + '_simtel'
  
  #building simtelArray Offset
  dictSimtelKW={}
  simtelConfigFile = open(simtelConfigFileName, "r").readlines()

  for line in simtelConfigFile:
    if (len(line.split())>0):
      if line[0] is not '#':
         for i in range(0,len(simtelKEYWORDS)-1):
           lineTuple = line.split(simtelKEYWORDS[i] +'=')[1].split(simtelKEYWORDS[i+1])
           dictSimtelKW[simtelKEYWORDS[i]] = lineTuple[0].strip()
          
  simtelOffset = dictSimtelKW['env offset']
  simtelCfg = dictSimtelKW['cfg']
  
    
  #building ParticleName
  dictParticleCode={}
  dictParticleCode['1'] = 'gamma'
  dictParticleCode['14'] = 'proton'
  dictParticleCode['3'] = 'electron'
  dictParticleCode['402'] = 'helium'
  dictParticleCode['1407'] = 'nitrogen'
  dictParticleCode['2814'] = 'silicon'
  dictParticleCode['5626'] = 'iron'

  prodName = corsikaDirPathMD['prodName']
  corsikaProdVersion = corsikaDirPathMD['corsikaProdVersion']
  pattern = "%s" % ('/'.join( [prodName,corsikaProdVersion,particle] ))
  pathroot = corsikaDirPath.split(pattern)[0]
  corsikaParticleDirPath = os.path.join(pathroot,pattern)
  simtelDirPath = os.path.join(corsikaParticleDirPath,simtelProdVersion+'_'+simtelConfig)

def createIndexes(indexesTypesDict):
  #CLAUDIAToBeDone: only if they don't already exist: waiting for Release update
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
   
 
def createSimtelFileSystAndMD():
  # Creating INDEXES in DFC DB
  simtelDirMDFields={}
  simtelDirMDFields['simtelArrayProdVersion'] = 'VARCHAR(128)'
  simtelDirMDFields['simtelArrayConfig'] = 'VARCHAR(128)'
  simtelDirMDFields['offset'] = 'float'
  createIndexes(simtelDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  simtelDirMD={}
  simtelDirMD['simtelArrayProdVersion'] = simtelProdVersion
  simtelDirMD['simtelArrayConfig'] = simtelConfig
  simtelOffsetCorr = simtelOffset[1:-1]
  simtelDirMD['offset'] = float(simtelOffsetCorr)

  res = createDirAndInsertMD(simtelDirPath, simtelDirMD)
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('MD Error: Problem creating Simtel Directory MD ')

  simtelDataDirPath = os.path.join(simtelDirPath,'Data')
  simtelDataDirMD={}
  simtelDataDirMD['outputType'] = 'Data'
  res = createDirAndInsertMD(simtelDataDirPath, simtelDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Data Directory MD ')
    
  simtelLogDirPath = os.path.join(simtelDirPath,'Log')
  simtelLogDirMD={}
  simtelLogDirMD['outputType'] = 'Log'
  res = createDirAndInsertMD(simtelLogDirPath, simtelLogDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Log Directory MD ')

  simtelHistoDirPath = os.path.join(simtelDirPath,'Histograms')
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

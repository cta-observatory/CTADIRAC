#!/usr/bin/env python
import DIRAC
import os

def setVersion( optionValue ):
  global version
  version = optionValue
  return DIRAC.S_OK()

def setConfig( optionValue ):
  global simtelConfig
  simtelConfig = optionValue
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

  Script.registerSwitch( "S:", "simtelConfig=", "SimtelConfig", setConfig )
  Script.registerSwitch( "V:", "version=", "Version", setVersion )

  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  Script.parseCommandLine()
  DIRAC.gLogger.setLevel('INFO')

  global fcc, fcL

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from DIRAC.Core.Utilities.Subprocess import systemCall
  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
  
  global jobID
  jobID = os.environ['JOBID']
  jobReport = JobReport( int(jobID) )

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
  
  install_CorsikaSimtelPack(version)
  #############
  # simtelConfigFile should be built from ???
  #simtelConfigFilesPath = 'sim_telarray/multi'
  #simtelConfigFile = simtelConfigFilesPath + '/multi_cta-ultra5.cfg'                          
  #createGlobalsFromConfigFiles(simtelConfigFile)
  #createGlobalsFromConfigFiles(current_version)
  #######################  
## files spread in 1000-runs subDirectories

  global corsikaFileLFN 
  corsikaFileLFN = dirac.getJobJDL(jobID)['Value']['InputData']
  print 'corsikaFileLFN is ' + corsikaFileLFN
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

############ Producing SimTel File
 ######################Building simtel Directory Metadata #######################

  cfg_dict = {"4MSST":'cta-prod2-4m-dc',"SCSST":'cta-prod2-sc-sst',"STD":'cta-prod2',"NSBX3":'cta-prod2',"ASTRI":'cta-prod2-astri',"SCMST":'cta-prod2-sc3',"NORTH":'cta-prod2n'}

  if simtelConfig=="6INROW":
    all_configs=["4MSST","SCSST","ASTRI","NSBX3","STD","SCMST"]
  elif simtelConfig=="5INROW":
    all_configs=["4MSST","SCSST","ASTRI","NSBX3","STD"]
  elif simtelConfig=="3INROW":
    all_configs=["SCSST","STD","SCMST"]
  else:
    all_configs=[simtelConfig]

  for current_conf in all_configs:

    DIRAC.gLogger.notice('current conf is',current_conf)

    if current_conf == "SCMST":
      current_version = version + '_sc3'
      DIRAC.gLogger.notice('current version is', current_version)
      if os.path.isdir('sim_telarray'):
        DIRAC.gLogger.notice('Package found in the local area. Removing package...')
        cmd = 'rm -R sim_telarray corsika-6990 hessioxxx corsika-run'
        if(os.system(cmd)):
          DIRAC.exit( -1 )
        install_CorsikaSimtelPack(current_version)
    else:
      current_version = version
      DIRAC.gLogger.notice('current version is', current_version)

########################################################

    createGlobalsFromConfigFiles(current_version)

    resultCreateSimtelDirMD = createSimtelFileSystAndMD(current_conf,current_version)
    if not resultCreateSimtelDirMD['OK']:
      DIRAC.gLogger.error( 'Failed to create simtelArray Directory MD')
      jobReport.setApplicationStatus('Failed to create simtelArray Directory MD')
      DIRAC.gLogger.error('Metadata coherence problem, no simtelArray File produced')
      DIRAC.exit( -1 )
    else:
      print 'simtel Directory MD successfully created'

############## introduce file existence check here ########################
    simtelFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.simtel.gz'
    simtelDirPath_conf = simtelDirPath + '_' + current_conf
    simtelOutFileDir = os.path.join(simtelDirPath_conf,'Data',runNumSeriesDir)
    simtelOutFileLFN = os.path.join(simtelOutFileDir,simtelFileName)

    res = CheckCatalogCoherence(simtelOutFileLFN)
    if res == DIRAC.S_OK:
      DIRAC.gLogger.notice('Current conf already done', current_conf)
      continue

#### execute simtelarray ################
    fd = open('run_sim.sh', 'w' )
    fd.write( """#! /bin/sh  
  export SVNPROD2=$PWD
  export SVNTAG=SVN-PROD2
  export CORSIKA_IO_BUFFER=800MB
  ./grid_prod2-repro.sh %s %s""" % (corsikaFileName,current_conf))
    fd.close()

    os.system('chmod u+x grid_prod2-repro.sh')
    os.system('chmod u+x run_sim.sh')
    cmdTuple = ['./run_sim.sh']
    ret = systemCall( 0, cmdTuple, sendOutputSimTel)
    simtelReturnCode, stdout, stderr = ret['Value']

    if(os.system('grep Broken simtel.log')):
      DIRAC.gLogger.notice('not broken')
    else:
      DIRAC.gLogger.notice('broken')
      jobReport.setApplicationStatus('Broken pipe')
      DIRAC.exit( -1 )

    if not ret['OK']:
      DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
      DIRAC.gLogger.error( 'run_sim.sh status is:', simtelReturnCode)
      DIRAC.exit( -1 )

## putAndRegister simtel data/log/histo Output File:
    cfg = cfg_dict[current_conf] 
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Data/*.simtel.gz ' + simtelFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

############################################
    simtelRunNumberSeriesDirExist = fcc.isDirectory(simtelOutFileDir)['Value']['Successful'][simtelOutFileDir]
    newSimtelRunFileSeriesDir = (simtelRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

    simtelLogFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.log.gz'
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Log/*.log.gz ' + simtelLogFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )
    simtelOutLogFileDir = os.path.join(simtelDirPath_conf,'Log',runNumSeriesDir)
    simtelOutLogFileLFN = os.path.join(simtelOutLogFileDir,simtelLogFileName)

    simtelHistFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.hdata.gz'
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Histograms/*.hdata.gz ' + simtelHistFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )
    simtelOutHistFileDir = os.path.join(simtelDirPath_conf,'Histograms',runNumSeriesDir)
    simtelOutHistFileLFN = os.path.join(simtelOutHistFileDir,simtelHistFileName) 

########### quality check on Histo Missing because it needs the NSHOW ############################################# 
########## quality check on Log #############################
    cmd = 'zcat %s | grep Finished.' % simtelLogFileName
    DIRAC.gLogger.notice( 'Executing system call:', cmd )
    if(os.system(cmd)):
      jobReport.setApplicationStatus('Log check Failed')
      DIRAC.exit( -1 )

################################################  
    from DIRAC.Core.Utilities import List
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    opsHelper = Operations()
    
    global seList
    seList = opsHelper.getValue( 'ProductionOutputs/SimtelProd', [] )
    seList  = List.randomize( seList )

    DIRAC.gLogger.notice('SeList is:',seList)

#########  Upload simtel data/log/histo ##############################################
    res = upload_to_seList(simtelOutFileLFN,simtelFileName)

    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('OutputData Upload Error',simtelOutFileLFN)
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )

    res = CheckCatalogCoherence(simtelOutLogFileLFN)
    if res == DIRAC.S_OK:
      DIRAC.gLogger.notice('Log file already exists. Removing:',simtelOutLogFileLFN)
      ret = dirac.removeFile( simtelOutLogFileLFN )

    res = upload_to_seList(simtelOutLogFileLFN,simtelLogFileName)

    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('Upload simtel Log Error',simtelOutLogFileLFN)
      DIRAC.gLogger.notice('Removing simtel data file:',simtelOutFileLFN)
      ret = dirac.removeFile( simtelOutFileLFN )
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )

    res = CheckCatalogCoherence(simtelOutHistFileLFN)
    if res == DIRAC.S_OK:
      DIRAC.gLogger.notice('Histo file already exists. Removing:',simtelOutHistFileLFN)
      ret = dirac.removeFile( simtelOutHistFileLFN )

    res = upload_to_seList(simtelOutHistFileLFN,simtelHistFileName)

    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('Upload simtel Histo Error',simtelOutHistFileLFN)
      DIRAC.gLogger.notice('Removing simtel data file:',simtelOutFileLFN)
      ret = dirac.removeFile( simtelOutFileLFN )
      DIRAC.gLogger.notice('Removing simtel log file:',simtelOutLogFileLFN)
      ret = dirac.removeFile( simtelOutLogFileLFN )
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )
####################################################################
    
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

def install_CorsikaSimtelPack(version):

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from DIRAC.Core.Utilities.Subprocess import systemCall

  CorsikaSimtelPack = os.path.join('corsika_simhessarray',version,'corsika_simhessarray')

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
      if installSoftwarePackage( package, workingArea() )['OK']:
      ############## compile #############################
        if 'sc3' in version:
          compilation_opt = 'sc3'
        else:
          compilation_opt = 'prod2'

        DIRAC.gLogger.notice( 'Compiling with option:',compilation_opt)
        cmdTuple = ['./build_all',compilation_opt,'qgs2']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to execute build')
          DIRAC.exit( -1 )
        continue
    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  

def upload_to_seList(FileLFN,FileName):

  DIRAC.gLogger.notice( 'Put and register in LFC and DFC:', FileLFN)
  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
  result = getSEsForSite( DIRAC.siteName() )
  if result['OK']:
    localSEs = result['Value']

  dirac = Dirac()
  upload_result = 'NOTOK'
  failing_se = []

  for se in localSEs:
    if se in seList:
      DIRAC.gLogger.notice( 'Local SE is in the list:',se)
      ret = dirac.addFile( FileLFN, FileName, se )
      res = CheckCatalogCoherence(FileLFN)
      if res != DIRAC.S_OK:
        DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
        DIRAC.gLogger.notice('Failing SE:',se)
        failing_se.append(se)
        continue
      upload_result = 'OK'

  if upload_result != 'OK':
    for se in seList:
      DIRAC.gLogger.notice('Try upload to:',se)
      ret = dirac.addFile( FileLFN, FileName, se )   

      res = CheckCatalogCoherence(FileLFN)
      if res != DIRAC.S_OK:
        DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
        failing_se.append(se)
        DIRAC.gLogger.notice('Failing SE:',se)
        continue
      upload_result = 'OK'
      break

  DIRAC.gLogger.notice('Failing SE list:',failing_se)

  if upload_result != 'OK':
    return DIRAC.S_ERROR

  return DIRAC.S_OK


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

def createGlobalsFromConfigFiles(current_version):

  global prodName
  global thetaP
  global phiP
  global particle
  global energyInfo
  global viewCone
  global pathroot
  global simtelOffset
  global corsikaDirPath
  global corsikaParticleDirPath
  global simtelDirPath
  global corsikaProdVersion
  #global simtelProdVersion
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

  simtelKEYWORDS = ['env offset']

  # Formatting MD values retrieved in configFiles
  #corsikaProdVersion = version + '_corsika' ## not used ### comment
  #simtelProdVersion = version + '_simtel'
  simtelProdVersion = current_version + '_simtel'

  #building simtelArray Offset
  dictSimtelKW={}
#  simtelConfigFile = open(simtelConfigFileName, "r").readlines()
#  for line in simtelConfigFile:
#    lineSplitEqual = line.split('=')
#    isAComment = '#' in lineSplitEqual[0].split()
#    for word in lineSplitEqual:
#      if (word in simtelKEYWORDS and not isAComment) :
#        offset = lineSplitEqual[1].split()[0]
#        dictSimtelKW[word] = offset

#  simtelOffset = dictSimtelKW['env offset']
  simtelOffset = '"0.0"'
    
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
#  corsikaParticleDirPath = "%s%s" % (pathroot,pattern)
  corsikaParticleDirPath = os.path.join(pathroot,pattern)
  #simtelDirPath = os.path.join(corsikaParticleDirPath,simtelProdVersion+'_'+simtelConfig)
  simtelDirPath = os.path.join(corsikaParticleDirPath,simtelProdVersion)

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
   
def createSimtelFileSystAndMD(current_conf, current_version):
  # Creating INDEXES in DFC DB
  simtelDirMDFields={}
  simtelDirMDFields['simtelArrayProdVersion'] = 'VARCHAR(128)'
  simtelDirMDFields['simtelArrayConfig'] = 'VARCHAR(128)'
  simtelDirMDFields['offset'] = 'float'
  createIndexes(simtelDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  simtelDirMD={}
  simtelProdVersion = current_version + '_simtel'
  simtelDirMD['simtelArrayProdVersion'] = simtelProdVersion
  #simtelDirMD['simtelArrayConfig'] = simtelConfig
  simtelDirMD['simtelArrayConfig'] = current_conf
  simtelOffsetCorr = simtelOffset[1:-1]
  simtelDirMD['offset'] = float(simtelOffsetCorr)

  simtelDirPath_conf = simtelDirPath + '_' + current_conf
  #res = createDirAndInsertMD(simtelDirPath, simtelDirMD)
  res = createDirAndInsertMD(simtelDirPath_conf, simtelDirMD)
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('MD Error: Problem creating Simtel Directory MD ')

  simtelDataDirPath = os.path.join(simtelDirPath_conf,'Data')
  simtelDataDirMD={}
  simtelDataDirMD['outputType'] = 'Data'
  res = createDirAndInsertMD(simtelDataDirPath, simtelDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Data Directory MD ')
    
  simtelLogDirPath = os.path.join(simtelDirPath_conf,'Log')
  simtelLogDirMD={}
  simtelLogDirMD['outputType'] = 'Log'
  res = createDirAndInsertMD(simtelLogDirPath, simtelLogDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Log Directory MD ')

  simtelHistoDirPath = os.path.join(simtelDirPath_conf,'Histograms')
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

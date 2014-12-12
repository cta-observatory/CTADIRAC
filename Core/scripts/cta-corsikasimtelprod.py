#!/usr/bin/env python
import DIRAC
import os
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

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

def setSaveCorsika( optionValue ):
  global savecorsika
  savecorsika = optionValue
  return DIRAC.S_OK()

def setConfig( optionValue ):
  global simtelConfig
  simtelConfig = optionValue
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
  Script.registerSwitch( "S:", "simtelConfig=", "SimtelConfig", setConfig )
  Script.registerSwitch( "V:", "version=", "Version", setVersion )
  Script.registerSwitch( "M:", "mode=", "Mode", setMode )
  Script.registerSwitch( "C:", "savecorsika=", "Save Corsika", setSaveCorsika )

  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  Script.parseCommandLine()
  global fcc, fcL, storage_element
  
  from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from CTADIRAC.Core.Workflow.Modules.CorsikaApp import CorsikaApp
  from CTADIRAC.Core.Workflow.Modules.Read_CtaApp import Read_CtaApp
  from DIRAC.Core.Utilities.Subprocess import systemCall

  jobID = os.environ['JOBID']
  jobID = int( jobID )
  global jobReport
  jobReport = JobReport( jobID )


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
  createGlobalsFromConfigFiles('prodConfigFile', corsikaTemplate,version)
  
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
  global CorsikaSimtelPack
  CorsikaSimtelPack = os.path.join('corsika_simhessarray', version, 'corsika_simhessarray')
  install_CorsikaSimtelPack(version, 'sim')
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
  corsikaOutFileDir = os.path.join(corsikaDirPath,particle,'Data',runNumSeriesDir)
  corsikaOutFileLFN = os.path.join(corsikaOutFileDir,corsikaFileName)
  corsikaRunNumberSeriesDirExist = fcc.isDirectory(corsikaOutFileDir)['Value']['Successful'][corsikaOutFileDir]
  newCorsikaRunNumberSeriesDir = (corsikaRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

#### create a file to DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK ################
  f = open('DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'w' )
  f.close()

  if savecorsika == 'True':
    DIRAC.gLogger.notice( 'Put and register corsika File in LFC and DFC:', corsikaOutFileLFN)
    ret = dirac.addFile(corsikaOutFileLFN, corsikaFileName, storage_element)  
  
    res = CheckCatalogCoherence(corsikaOutFileLFN)

    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )
    
    if not ret['OK']:
      DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )  
    
  # put and register corsikaTarFile:
    corsikaTarFileDir = os.path.join(corsikaDirPath,particle,'Log',runNumSeriesDir)
    corsikaTarFileLFN = os.path.join(corsikaTarFileDir,corsikaTarName)

##### If storage element is IN2P3-tape save simtel file on disk ###############  
    if storage_element == 'CC-IN2P3-Tape':
      storage_element = 'CC-IN2P3-Disk'

    DIRAC.gLogger.notice( 'Put and register corsikaTar File in LFC and DFC:', corsikaTarFileLFN)
    ret = dirac.addFile(corsikaTarFileLFN, corsikaTarName, storage_element)
  
####Checking and restablishing catalog coherence #####################  
    res = CheckCatalogCoherence(corsikaTarFileLFN)
    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('Job failed: Catalog Coherence problem found')
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )
     
    if not ret['OK']:
      DIRAC.gLogger.error('Error during addFile call:', ret['Message'])
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )
######################################################################
      
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

  cfg_dict = {"4MSST":'cta-prod2-4m-dc',"SCSST":'cta-prod2-sc-sst',"STD":'cta-prod2',"NSBX3":'cta-prod2',"ASTRI":'cta-prod2-astri',"SCMST":'cta-prod2-sc3',"NORTH":'cta-prod2n'}

  if simtelConfig=="6INROW":
    all_configs=["4MSST","SCSST","ASTRI","NSBX3","STD","SCMST"]
  elif simtelConfig=="5INROW":
    all_configs=["4MSST","SCSST","ASTRI","NSBX3","STD"]
  elif simtelConfig=="3INROW":
    all_configs=["SCSST","STD","SCMST"]
  else:
    all_configs=[simtelConfig]

############################################
  #for current_conf in all_configs:
    #DIRAC.gLogger.notice('current conf is',current_conf)
    #if current_conf == "SCMST":
      #current_version = version + '_sc3'
      #DIRAC.gLogger.notice('current version is', current_version)
      #if os.path.isdir('sim_telarray'):
        #DIRAC.gLogger.notice('Package found in the local area. Removing package...')
        #cmd = 'rm -R sim_telarray corsika-6990 hessioxxx corsika-run'
        #if(os.system(cmd)):
          #DIRAC.exit( -1 )
        #install_CorsikaSimtelPack(current_version)
    #else:
      #current_version = version
      #DIRAC.gLogger.notice('current version is', current_version)
#############################################################

  for current_conf in all_configs:
    DIRAC.gLogger.notice('current conf is',current_conf)
    if current_conf == "SCMST":
      current_version = version + '_sc3'
      DIRAC.gLogger.notice('current version is', current_version)
      installSoftwareEnviron( CorsikaSimtelPack, workingArea(), 'sim-sc3')
    else:
      current_version = version
      DIRAC.gLogger.notice('current version is', current_version)

########################################################

    global simtelDirPath
    global simtelProdVersion

    simtelProdVersion = current_version + '_simtel'
    simtelDirPath = os.path.join(corsikaParticleDirPath,simtelProdVersion)
  
    resultCreateSimtelDirMD = createSimtelFileSystAndMD(current_conf)
    if not resultCreateSimtelDirMD['OK']:
      DIRAC.gLogger.error( 'Failed to create simtelArray Directory MD')
      jobReport.setApplicationStatus('Failed to create simtelArray Directory MD')
      DIRAC.gLogger.error('Metadata coherence problem, no simtelArray File produced')
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice('simtel Directory MD successfully created')

############## check simtel data file LFN exists ########################
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
source ./Corsika_simhessarrayEnv.sh
export SVNPROD2=$PWD
export SVNTAG=SVN-PROD2_rev10503
export CORSIKA_IO_BUFFER=800MB
cp ../grid_prod2-repro.sh .
ln -s ../%s
ln -s ../$SVNTAG
./grid_prod2-repro.sh %s %s""" % (corsikaFileName,corsikaFileName,current_conf))
    fd.close()
####################################

    os.system('chmod u+x run_sim.sh')
    cmdTuple = ['./run_sim.sh']
    ret = systemCall( 0, cmdTuple, sendOutputSimTel)
    simtelReturnCode, stdout, stderr = ret['Value']

    if(os.system('grep Broken simtel.log')==0):
      DIRAC.gLogger.error('Broken string found in simtel.log')
      jobReport.setApplicationStatus('Broken pipe')
      DIRAC.exit( -1 )

    if not ret['OK']:
      DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
      DIRAC.gLogger.error( 'run_sim.sh status is:', simtelReturnCode)
      DIRAC.exit( -1 )

##   check simtel data/log/histo Output File exist
    cfg = cfg_dict[current_conf]
    #cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Data/*.simtel.gz ' + simtelFileName
    if current_conf == "SCMST":
      cmdprefix = 'mv sim-sc3/Data/sim_telarray/' + cfg + '/0.0deg/'
    else:
      cmdprefix = 'mv sim/Data/sim_telarray/' + cfg + '/0.0deg/'

    cmd = cmdprefix + 'Data/*'+ cfg + '_*.simtel.gz ' + simtelFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

############################################
    simtelRunNumberSeriesDirExist = fcc.isDirectory(simtelOutFileDir)['Value']['Successful'][simtelOutFileDir]
    newSimtelRunFileSeriesDir = (simtelRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

    simtelLogFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.log.gz'
    #cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Log/*.log.gz ' + simtelLogFileName
    cmd = cmdprefix + 'Log/*'+ cfg + '_*.log.gz ' + simtelLogFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )
    simtelOutLogFileDir = os.path.join(simtelDirPath_conf,'Log',runNumSeriesDir)
    simtelOutLogFileLFN = os.path.join(simtelOutLogFileDir,simtelLogFileName)

    simtelHistFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.hdata.gz'
    #cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Histograms/*.hdata.gz ' + simtelHistFileName
    cmd = cmdprefix + 'Histograms/*'+ cfg + '_*.hdata.gz ' + simtelHistFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )
    simtelOutHistFileDir = os.path.join(simtelDirPath_conf,'Histograms',runNumSeriesDir)
    simtelOutHistFileLFN = os.path.join(simtelOutHistFileDir,simtelHistFileName)

########### quality check on Histo ############################################# 
    fd = open('check_histo.sh', 'w' )
    fd.write( """#! /bin/sh  
nsim=$(list_histograms %s|fgrep 'Histogram 6 '|sed 's/^.*contents: //'| sed 's:/.*$::')
nevents=%d
if [ $nsim -lt $(( $nevents - 20 )) ]; then
echo 'nsim found:' $nsim
echo 'nsim expected:' $nevents
exit 1
else
echo 'nsim found:' $nsim
echo 'nsim expected:' $nevents
fi
""" % (simtelHistFileName,int(nbShowers)*int(cscat)))
    fd.close()

    ret = getSoftwareEnviron( CorsikaSimtelPack )

    if not ret['OK']:
      error = ret['Message']
      DIRAC.gLogger.error( error, CorsikaSimtelPack )
      DIRAC.exit( -1 )

    corsikaEnviron = ret['Value']

    os.system('chmod u+x check_histo.sh')
    cmdTuple = ['./check_histo.sh']
    DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
    ret = systemCall( 0, cmdTuple, sendOutput,env = corsikaEnviron)
    checkHistoReturnCode, stdout, stderr = ret['Value']

    if not ret['OK']:
      DIRAC.gLogger.error( 'Failed to execute check_histo.sh')
      DIRAC.gLogger.error( 'check_histo.sh status is:', checkHistoReturnCode)
      DIRAC.exit( -1 )

    if (checkHistoReturnCode!=0):
      DIRAC.gLogger.error( 'Failure during check_histo.sh')
      DIRAC.gLogger.error( 'check_histo.sh status is:', checkHistoReturnCode)
      jobReport.setApplicationStatus('Histo check Failed')
      DIRAC.exit( -1 )

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

#    simtelRunNumberSeriesDirExist = fcc.isDirectory(simtelOutFileDir)['Value']['Successful'][simtelOutFileDir]
#    newSimtelRunFileSeriesDir = (simtelRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD
    
    if newSimtelRunFileSeriesDir:
      print 'insertRunFileSeriesMD'
      insertRunFileSeriesMD(simtelOutFileDir,runNumTrunc)
      insertRunFileSeriesMD(simtelOutLogFileDir,runNumTrunc)
      insertRunFileSeriesMD(simtelOutHistFileDir,runNumTrunc)
    else:
      print 'NotinsertRunFileSeriesMD'
    
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

    if savecorsika == 'True':
      result = fcc.addFileAncestors({simtelOutFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
      print 'result addFileAncestor:', result

      result = fcc.addFileAncestors({simtelOutLogFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
      print 'result addFileAncestor:', result

      result = fcc.addFileAncestors({simtelOutHistFileLFN:{'Ancestors': [ corsikaOutFileLFN ] }})
      print 'result addFileAncestor:', result

#####  Exit now if only corsika simulation required
    if (mode == 'corsika_simtel'):
      continue

######### run read_cta #######################################

    rcta = Read_CtaApp()
    rcta.setSoftwarePackage(CorsikaSimtelPack)
    rcta.rctaExe = 'read_cta'

    powerlaw_dict = {'gamma':'-2.57','gamma_ptsrc':'-2.57','proton':'-2.70','electron':'-3.21'}
    dstFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.simtel-dst0.gz'
    dstHistoFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.hdata-dst0.gz'

## added some options starting from Armazones_2K prod.
    rcta.rctaArguments = ['-r', '4', '-u', '--integration-scheme', '4', '--integration-window', '7,3', '--tail-cuts', '6,8', '--min-pix', '2', '--min-amp', '20', '--type', '1,0,0,400', '--tail-cuts', '9,12', '--min-amp', '20', '--type', '2,0,0,100', '--tail-cuts', '8,11', '--min-amp', '19', '--type', '3,0,0,40', '--tail-cuts', '6,9', '--min-amp', '15', '--type', '4,0,0,15', '--tail-cuts', '3.7,5.5', '--min-amp', '8', '--type', '5,0,0,70,5.6', '--tail-cuts', '2.4,3.2', '--min-amp', '5.6', '--dst-level', '0', '--dst-file', dstFileName, '--histogram-file', dstHistoFileName, '--powerlaw', powerlaw_dict[particle], simtelFileName]

    rctaReturnCode = rcta.execute()
  
    if rctaReturnCode != 0:
      DIRAC.gLogger.error( 'read_cta Application: Failed')
      jobReport.setApplicationStatus('read_cta Application: Failed')
      DIRAC.exit( -1 )

######## run dst quality checks ######################################

    fd = open('check_dst_histo.sh', 'w' )
    fd.write( """#! /bin/sh  
dsthistfilename=%s
dstfile=%s
n6="$(list_histograms -h 6 ${dsthistfilename} | grep 'Histogram of type' | sed 's/.*bins, //' | sed 's/ entries.//')" 
n12001="$(list_histograms -h 12001 ${dsthistfilename} | grep 'Histogram of type' | sed 's/.*bins, //' | sed 's/ entries.//')" 
if [ $n6 -ne $n12001 ]; then
echo 'n6 found:' $n6
echo 'n12001 found:' $n12001
exit 1
else
echo 'n6 found:' $n6
echo 'n12001 found:' $n12001
fi

n12002="$(list_histograms -h 12002 ${dsthistfilename} | grep 'Histogram of type' | sed 's/.*bins, //' | sed 's/ entries.//')" 
nev="$(statio ${dstfile} | egrep '^2010' | cut -f2)"
if [ -z "$nev" ]; then nev="0"; fi

if [ $nev -ne $n12002 ]; then
echo 'nev found:' $nev
echo 'n12002 found:' $n12002
exit 1
else
echo 'nev found:' $nev
echo 'n12002 found:' $n12002
fi
""" % (dstHistoFileName,dstFileName))
    fd.close()

    os.system('chmod u+x check_dst_histo.sh')
    cmdTuple = ['./check_dst_histo.sh']
    DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
    ret = systemCall( 0, cmdTuple, sendOutput,env = corsikaEnviron)
    checkHistoReturnCode, stdout, stderr = ret['Value']

    if not ret['OK']:
      DIRAC.gLogger.error( 'Failed to execute check_dst_histo.sh')
      DIRAC.gLogger.error( 'check_dst_histo.sh status is:', checkHistoReturnCode)
      DIRAC.exit( -1 )

    if (checkHistoReturnCode!=0):
      DIRAC.gLogger.error( 'Failure during check_dst_histo.sh')
      DIRAC.gLogger.error( 'check_dst_histo.sh status is:', checkHistoReturnCode)
      jobReport.setApplicationStatus('Histo check Failed')
      DIRAC.exit( -1 )

############create MD and upload dst data/histo ##########################################################

    global dstDirPath
    global dstProdVersion

    dstProdVersion = current_version + '_dst'
    dstDirPath = os.path.join(simtelDirPath_conf,dstProdVersion)

    dstOutFileDir = os.path.join(dstDirPath,'Data',runNumSeriesDir)
    dstOutFileLFN = os.path.join(dstOutFileDir,dstFileName)

    resultCreateDstDirMD = createDstFileSystAndMD()
    if not resultCreateDstDirMD['OK']:
      DIRAC.gLogger.error( 'Failed to create Dst Directory MD')
      jobReport.setApplicationStatus('Failed to create Dst Directory MD')
      DIRAC.gLogger.error('Metadata coherence problem, no Dst File produced')
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice('Dst Directory MD successfully created')
############################################################

    res = CheckCatalogCoherence(dstOutFileLFN)
    if res == DIRAC.S_OK:
      DIRAC.gLogger.notice('dst file already exists. Removing:',dstOutFileLFN)
      ret = dirac.removeFile( dstOutFileLFN )

    res = upload_to_seList(dstOutFileLFN,dstFileName)

    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('Upload dst Error',dstOutFileLFN)
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )

##############################################################
    dstHistoFileDir = os.path.join(dstDirPath,'Histograms',runNumSeriesDir)
    dstHistoFileLFN = os.path.join(dstHistoFileDir,dstHistoFileName)

    res = CheckCatalogCoherence(dstHistoFileLFN)
    if res == DIRAC.S_OK:
      DIRAC.gLogger.notice('dst histo file already exists. Removing:',dstHistoFileLFN)
      ret = dirac.removeFile( dstHistoFileLFN )

    res = upload_to_seList(dstHistoFileLFN,dstHistoFileName)

    if res != DIRAC.S_OK:
      DIRAC.gLogger.error('Upload dst Error',dstHistoFileName)
      jobReport.setApplicationStatus('OutputData Upload Error')
      DIRAC.exit( -1 )

########### Insert RunNumSeries MD ##########################

    dstRunNumberSeriesDirExist = fcc.isDirectory(dstOutFileDir)['Value']['Successful'][dstOutFileDir]
    newDstRunFileSeriesDir = (dstRunNumberSeriesDirExist != True)  # if new runFileSeries, will need to add new MD

    if newDstRunFileSeriesDir:
      insertRunFileSeriesMD(dstOutFileDir,runNumTrunc)
      insertRunFileSeriesMD(dstHistoFileDir,runNumTrunc)

####### dst File level metadata ###############################################
    dstFileMD={}
    dstFileMD['runNumber'] = int(run_number)
    dstFileMD['jobID'] = jobID
    dstFileMD['rctaReturnCode'] = rctaReturnCode
  
    result = fcc.setMetadata(dstOutFileLFN,dstFileMD)
    print "result setMetadata=",result
    if not result['OK']:
      print 'ResultSetMetadata:',result['Message']

    result = fcc.setMetadata(dstHistoFileLFN,dstFileMD)
    print "result setMetadata=",result
    if not result['OK']:
      print 'ResultSetMetadata:',result['Message']

########## set the ancestors for dst #####################################

    result = fcc.addFileAncestors({dstOutFileLFN:{'Ancestors': [ simtelOutFileLFN] }})
    print 'result addFileAncestor:', result

    result = fcc.addFileAncestors({dstHistoFileLFN:{'Ancestors': [ simtelOutFileLFN] }})
    print 'result addFileAncestor:', result

######################################################
    
  DIRAC.exit()


def install_CorsikaSimtelPack(version, build_dir):

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from DIRAC.Core.Utilities.Subprocess import systemCall

  packs = [CorsikaSimtelPack]
  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        installSoftwareEnviron( package, workingArea(), build_dir)
        packageTuple =  package.split('/')
        corsika_subdir = os.path.join(sharedArea(),packageTuple[0],version) 
        cmd = 'cp -u -r ' + corsika_subdir + '/* .'       
        if(os.system(cmd)):
          DIRAC.exit( -1 )
        continue
    if workingArea:
      print 'workingArea is %s ' % workingArea()
      if installSoftwarePackage( package, workingArea(), extract = False )['OK']:
        fd = open('run_compile.sh', 'w' )
        fd.write( """#! /bin/sh      
current_dir=%s
mkdir sim sim-sc3
(cd sim && tar zxvf ${current_dir}/corsika_simhessarray.tar.gz && ./build_all prod2 qgs2)
(cd sim-sc3 && tar zxvf ${current_dir}/corsika_simhessarray.tar.gz && ./build_all sc3 qgs2)""" % (workingArea()))
        fd.close()
        os.system('chmod u+x run_compile.sh')
        #os.system('cat run_compile.sh')
        cmdTuple = ['./run_compile.sh']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to compile')
          DIRAC.exit( -1 )
        installSoftwareEnviron( package, workingArea(), build_dir )
        continue

    DIRAC.gLogger.error( 'Software package not correctly installed')
    DIRAC.exit( -1 ) 

  return DIRAC.S_OK

def CheckCatalogCoherence(fileLFN):
####Checking and restablishing catalog coherence #####################  
  res = fcc.getReplicas(fileLFN)  

  if not res['OK']:
    DIRAC.gLogger.error('getReplicas res not OK:',fileLFN)
    return DIRAC.S_ERROR

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

  #for se in failing_se:
  #  seList.remove(se)

#  DIRAC.gLogger.notice('Failing SE list:',failing_se)
  if upload_result != 'OK':
    return DIRAC.S_ERROR

  return DIRAC.S_OK
     
def createGlobalsFromConfigFiles(prodConfigFileName, corsikaConfigFileName,version):

  global prodName
  global thetaP
  global phiP
  global particle
  global energyInfo
  global viewCone
  global pathroot
  global nbShowers
  global cscat
  global simtelOffset
  global prodDirPath
  global corsikaDirPath
  global corsikaParticleDirPath
  #global simtelDirPath
  global corsikaOutputFileName
  global corsikaProdVersion
  #global simtelProdVersion
  global obslev

  # Getting MD Values from Config Files:
  prodKEYWORDS =  ['prodName','pathroot']
  dictProdKW = fileToKWDict(prodConfigFileName,prodKEYWORDS)

  corsikaKEYWORDS = ['THETAP', 'PHIP', 'PRMPAR', 'ESLOPE' , 'ERANGE', 'VIEWCONE','NSHOW','TELFIL','OBSLEV','CSCAT']
  dictCorsikaKW = fileToKWDict(corsikaConfigFileName,corsikaKEYWORDS)

  #simtelKEYWORDS = ['env offset']

  # Formatting MD values retrieved in configFiles
  prodName = dictProdKW['prodName'][0]
  corsikaProdVersion = version + '_corsika'
  #simtelProdVersion = version + '_simtel'
  thetaP = str(float(dictCorsikaKW['THETAP'][0]))
  phiP = str(float(dictCorsikaKW['PHIP'][0]))
  obslev = str(float(dictCorsikaKW['OBSLEV'][0])/100.)#why on earth is this in cm....
  nbShowers = str(int(dictCorsikaKW['NSHOW'][0]))
  cscat = str(int(dictCorsikaKW['CSCAT'][0]))
  corsikaOutputFileName = dictCorsikaKW['TELFIL'][0]  
  
  #building simtelArray Offset
  dictSimtelKW={}
  simtelOffset = '"0.0"'
  
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
  prodDirPath = os.path.join(pathroot,prodName)
  corsikaDirPath = os.path.join(prodDirPath,corsikaProdVersion)
  corsikaParticleDirPath = os.path.join(corsikaDirPath,particle)
  #simtelDirPath = os.path.join(corsikaParticleDirPath,simtelProdVersion)

  
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
    
  corsikaParticleDataDirPath = os.path.join(corsikaParticleDirPath,'Data')  
  corsikaParticleDataDirMD={}
  corsikaParticleDataDirMD['outputType'] = 'corsikaData'
  res = createDirAndInsertMD(corsikaParticleDataDirPath, corsikaParticleDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Particle Data Directory MD ')

  corsikaParticleLogDirPath = os.path.join(corsikaParticleDirPath,'Log')  
  corsikaParticleLogDirMD={}
  corsikaParticleLogDirMD['outputType'] = 'corsikaLog'
  res = createDirAndInsertMD(corsikaParticleLogDirPath, corsikaParticleLogDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Corsika Particle Log Directory MD ')
    
  return DIRAC.S_OK ('Corsika Directory MD successfully created')

#def createSimtelFileSystAndMD():
def createSimtelFileSystAndMD(current_conf):
  # Creating INDEXES in DFC DB
  simtelDirMDFields={}
  simtelDirMDFields['simtelArrayProdVersion'] = 'VARCHAR(128)'
  simtelDirMDFields['simtelArrayConfig'] = 'VARCHAR(128)'
  simtelDirMDFields['offset'] = 'float'
  createIndexes(simtelDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  simtelDirMD={}
  simtelDirMD['simtelArrayProdVersion'] = simtelProdVersion
#############" new
  simtelDirMD['simtelArrayConfig'] = current_conf
  simtelOffsetCorr = simtelOffset[1:-1]
  simtelDirMD['offset'] = float(simtelOffsetCorr)


  simtelDirPath_conf = simtelDirPath + '_' + current_conf
  #res = createDirAndInsertMD(simtelDirPath, simtelDirMD)
  res = createDirAndInsertMD(simtelDirPath_conf, simtelDirMD)
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('MD Error: Problem creating Simtel Directory MD ')
    
  #simtelDataDirPath = os.path.join(simtelDirPath,'Data')
  simtelDataDirPath = os.path.join(simtelDirPath_conf,'Data')
  simtelDataDirMD={}
  simtelDataDirMD['outputType'] = 'Data'
  res = createDirAndInsertMD(simtelDataDirPath, simtelDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Data Directory MD ')

  #simtelLogDirPath = os.path.join(simtelDirPath,'Log')
  simtelLogDirPath = os.path.join(simtelDirPath_conf,'Log')
  simtelLogDirMD={}
  simtelLogDirMD['outputType'] = 'Log'
  res = createDirAndInsertMD(simtelLogDirPath, simtelLogDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Log Directory MD ')

  #simtelHistoDirPath = os.path.join(simtelDirPath,'Histograms')
  simtelHistoDirPath = os.path.join(simtelDirPath_conf,'Histograms')
  simtelHistoDirMD={}
  simtelHistoDirMD['outputType'] = 'Histo'
  res = createDirAndInsertMD(simtelHistoDirPath, simtelHistoDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Simtel Histo Directory MD ')
    
  return DIRAC.S_OK ('Simtel Directory MD successfully created')


def createDstFileSystAndMD():
  # Creating INDEXES in DFC DB
  dstDirMDFields={}
  dstDirMDFields['dstProdVersion'] = 'VARCHAR(128)'
  createIndexes(dstDirMDFields)  
  
  # Adding Directory level metadata Values to DFC
  dstDirMD={}
  dstDirMD['dstProdVersion'] = dstProdVersion

  res = createDirAndInsertMD(dstDirPath, dstDirMD)
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('MD Error: Problem creating Dst Directory MD ')
    
  dstDataDirPath = os.path.join(dstDirPath,'Data')
  dstDataDirMD={}
  dstDataDirMD['outputType'] = 'dstData'
  res = createDirAndInsertMD(dstDataDirPath, dstDataDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Dst Data Directory MD ')

  dstHistoDirPath = os.path.join(dstDirPath,'Histograms')
  dstHistoDirMD={}
  dstHistoDirMD['outputType'] = 'dstHisto'
  res = createDirAndInsertMD(dstHistoDirPath, dstHistoDirMD)  
  if res != DIRAC.S_OK:
    return DIRAC.S_ERROR ('Problem creating Dst Histo Directory MD ')
    
  return DIRAC.S_OK ('Dst Directory MD successfully created')


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

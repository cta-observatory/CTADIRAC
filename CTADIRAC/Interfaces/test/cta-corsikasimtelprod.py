#!/usr/bin/env python
import DIRAC
import os


def setRunNumber( optionValue ):
  global run_number
  run_number = optionValue.split('ParametricParameters=')[1]
  return DIRAC.S_OK()
  
def setRun( optionValue ):
  global run
  run = optionValue
  return DIRAC.S_OK()

def setConfigPath( optionValue ):
  global config_path
  config_path = optionValue
  return DIRAC.S_OK()

def setTemplate( optionValue ):
  global template
  template = optionValue
  return DIRAC.S_OK()

def setExecutable( optionValue ):
  global executable
  executable = optionValue
  return DIRAC.S_OK()

def setVersion( optionValue ):
  global version
  version = optionValue
  return DIRAC.S_OK()


def sendOutput(stdid,line):
  logfilename = executable + '.log'
  f = open( logfilename,'a')
  f.write(line)
  f.write('\n')
  f.close()
  DIRAC.gLogger.notice(line)



def main():

  from DIRAC.Core.Base import Script

  Script.registerSwitch( "p:", "run_number=", "Run Number", setRunNumber )
  Script.registerSwitch( "R:", "run=", "Run", setRun )
  Script.registerSwitch( "P:", "config_path=", "Config Path", setConfigPath )
  Script.registerSwitch( "T:", "template=", "Template", setTemplate )
  Script.registerSwitch( "E:", "executable=", "Executable", setExecutable )
  Script.registerSwitch( "V:", "version=", "Version", setVersion )
  

  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  Script.parseCommandLine()
  fc = FileCatalog('LcgFileCatalog')


  res = fc._getCatalogConfigDetails('DIRACFileCatalog')
  print 'DFC CatalogConfigDetails:',res
  res = fc._getCatalogConfigDetails('LcgFileCatalog')
  print 'LCG CatalogConfigDetails:',res
  
  
  fcc = FileCatalogClient()

  res1 = fcc.listDirectory('/vo.cta.in2p3.fr/user/l/lavalley/lfc_test')
  print 'listDir:',res1
  print 'listDir. Value:', res1['Value']
  print 'listDir. Value.Successful:', res1['Value']['Successful']
  #res3 = fcc.getMetadataFields


  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

  jobID = os.environ['JOBID']
  jobID = int( jobID )
  jobReport = JobReport( jobID )


  from CTADIRAC.Core.Workflow.Modules.CorsikaApp import CorsikaApp
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea

  from DIRAC.Core.Utilities.Subprocess import systemCall

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
        cmd = 'cp -r ' + corsika_subdir + '/* .'        
        os.system(cmd)
        continue
    if workingArea:
      if checkSoftwarePackage( package, workingArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Local Area:', package )
        continue
      if installSoftwarePackage( package, workingArea() )['OK']:
      ############## compile #############################
        cmdTuple = ['./build_all','ultra','qgs2']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to execute build')
          DIRAC.exit( -1 )
        continue

    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  
    
  def myAddFile(lfn, fullPath, diracSE):
    from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
    from DIRAC.Core.Utilities.File import makeGuid, getSize
    dirac = Dirac()
    rm = ReplicaManager()
    
    # put and Register into LFC:
    #res1 = rm.putAndRegister(lfn, fullPath, diracSE, guid = None, catalog = 'LcgFileCatalog')
    #print "LFC putAndRegister result:",res1
    res1 = dirac.addFile(lfn, fullPath, diracSE)
    print "LFC addFile result:",res1
    
    if not res1['OK']:
      #return self.__errorReport( 'Problem during LFC putAndRegister call', result['Message'] )
      print 'Problem during LFC putAndRegister call', res1['Message'] 
      return DIRAC.S_ERROR ('Problem during LFC putAndRegister call %s' % res1['Message'] )
     
    # registerFile into DFC
    size = getSize( fullPath )
    fileTuple = (lfn, fullPath, size, diracSE, None, None)
    res2 = rm.registerFile( fileTuple, 'DIRACFileCatalog' )
    print "DFC Register result:",res2
    if not res2['OK']:
      #return self.__errorReport( 'Problem during LFC putAndRegister call', result['Message'] )
      print 'Problem during DFC registerFile call', res2['Message'] 
      return DIRAC.S_ERROR ('Problem during DFC registerFile call %s' % res2['Message'] )
      
    return DIRAC.S_OK ('myAddFile Successful')


  # Producing Corsika File
  cs = CorsikaApp()

  cs.setSoftwarePackage(CorsikaSimtelPack)

  cs.csExe = executable

  cs.csArguments = ['--run-number',run_number,'--run',run,template] 

  res = cs.execute()
  
  if not res['OK']:
    DIRAC.gLogger.error( 'Failed to execute corsika_simtelarray Application')
    jobReport.setApplicationStatus('Corsika_simtelarray Application: Failed')
    DIRAC.exit( -1 )

  particle = 'proton'
  rundir = 'run' + run_number
  filein = rundir + '/cta-ultra3-test.corsika.gz'
  corsikafilename = particle + '_' + 'run' + run_number +  '.corsika.gz'
  mv_cmd = 'mv ' + filein + ' ' + corsikafilename
  os.system(mv_cmd)

  from DIRAC.Interfaces.API.Dirac import Dirac
  dirac = Dirac()


  #################################METADATA
  
  def fileToKWDict (fileName, keywordsList):    
    print 'parsing %s...' % fileName
    dict={}
    configFile = open(fileName, "r").readlines()
    for line in configFile:
      for word in line.split():
        if word in keywordsList:
          print line
          lineSplit = line.split()
          lenLineSplit = len(lineSplit)
          value = lineSplit[1:lenLineSplit]
          print 'value:',value
          dict[word] = value
          print 'dict:',dict
    return dict
  
  # ProdConfig Directory MD:
  prodKEYWORDS =  ['prodName', 'corsikaProdVersion', 'simtelProdVersion']
  dictProdKW = fileToKWDict('prodConfigFile',prodKEYWORDS)
  
  # CorsikaConfig Directory MD:
  #reminder Corsika keywords: 
  #PRMPAR  14            // particle type of prim. particle
  #ESLOPE  -2.0          // slope of primary energy spectrum
  #ERANGE  0.005E3 500E3 // energy range of primary particle (in GeV)
  #THETAP  20.  20.      // range of zenith angles (degree)
  #PHIP    90. 90.       // range of azimuth angles (degree)
  #VIEWCONE 0. 10.       // can be a cone around fixed THETAP/PHIP
  corsikaKEYWORDS = ['THETAP', 'PHIP', 'PRMPAR', 'ESLOPE' , 'ERANGE', 'VIEWCONE','NSHOW']
  dictCorsikaKW = fileToKWDict('INPUTS_CTA_ULTRA3_proton',corsikaKEYWORDS)
  
  #Particle Codes in Corsika
  dictParticleCode={}
  dictParticleCode['1'] = 'gamma'
  dictParticleCode['14'] = 'proton'
  dictParticleCode['3'] = 'electron'
  

  # Formatting MD values retrieved from configFiles
  prodName = dictProdKW['prodName'][0]
  corsikaProdVersion = dictProdKW['corsikaProdVersion'][0]
  simtelProdVersion = dictProdKW['simtelProdVersion'][0]
  zen = str(float(dictCorsikaKW['THETAP'][0]))
  az = str(float(dictCorsikaKW['PHIP'][0]))
  particleCode = dictCorsikaKW['PRMPAR'][0]
  particle = dictParticleCode[particleCode]
  eslope = dictCorsikaKW['ESLOPE'][0]
  eRange = dictCorsikaKW['ERANGE']
  emin = eRange[0]
  emax = eRange[1]  
  energyInfo = eslope + '_' + emin + '-' + emax
  viewConeRange = dictCorsikaKW['VIEWCONE']
  viewCone = str(float(viewConeRange[1]))

  pathTuple = [prodName,zen,az,particle,energyInfo,viewCone,corsikaProdVersion]
  print 'pathTuple:',pathTuple
  pathroot = '/vo.cta.in2p3.fr/user/l/lavalley/DFC-test/MC/'  
  dirPath = "%s%s" % ( pathroot, '/'.join( pathTuple ) )  
  
  nbShowers = str(int(dictCorsikaKW['NSHOW'][0]))
  
  # Inserting Directory level metadata 
  corsikaDirMD={}
  corsikaDirMD['prodName'] = prodName
  corsikaDirMD['zen'] = zen
  corsikaDirMD['az'] = az
  corsikaDirMD['particle'] = particle  
  corsikaDirMD['energyInfo'] = energyInfo
  corsikaDirMD['viewCone'] = viewCone
  corsikaDirMD['corsikaProdVersion'] = corsikaProdVersion
  corsikaDirMD['nbShowers'] = nbShowers
  
  # user queries Metadata become searchable INDEXES here
  #result = fcc.addMetadataField('prodName','VARCHAR(32)')
  #print 'result addMetadataField prodName:',result
  #result = fcc.addMetadataField('zen','float')
  #print 'result addMetadataField zen:',result
  #result = fcc.addMetadataField('az','float')
  #print 'result addMetadataField az:',result
  #result = fcc.addMetadataField('particle','VARCHAR(16)')
  #print 'result addMetadataField particle:',result
  #result = fcc.addMetadataField('energyInfo','VARCHAR(16)')
  #print 'result addMetadataField energyInfo:',result
  #result = fcc.addMetadataField('viewCone','float')
  #print 'result addMetadataField viewCone:',result
  #result = fcc.addMetadataField('corsikaProdVersion','VARCHAR(32)')
  #print 'result addMetadataField corsikaProdVersion:',result
  
  # if directory already exist, verify if metadata already exist.  If not, create directory and associated MD 
  dirExists = fcc.isDirectory(dirPath)['Value']['Successful'][dirPath]
  print 'dirExist result',dirExists
  if (dirExists):
    print 'Directory already exists'
    dirMd = fcc.getDirectoryMetadata(dirPath)
    dirMdV = dirMd['Value']
    print 'Directory metadata:',dirMdV
    lenDirMD = len(dirMdV)
    print 'lenDirMD=',lenDirMD
    if lenDirMD > 0:  
      print 'Metadata Already exist'  
      print 'Metadata:',dirMd['Value']
    else: 
      #insert MD:
      print 'insert MD...'
      result = fcc.setMetadata(dirPath,corsikaDirMD)
      print 'result setMetadataDir:',result
  else:
    print 'New directory, creating path '
    res = fcc.createDirectory(dirPath)
    print "createDir res:", res
    # insert Directory level MD:
    result = fcc.setMetadata(dirPath,corsikaDirMD)
    print 'result setMetadataDir:',result
    
  
  corsikaparamTuple = [prodName,zen,az,particle,energyInfo,viewCone,corsikaProdVersion,corsikafilename]
  corsikaoutfileLFN = "%s%s" % ( pathroot, '/'.join( corsikaparamTuple ) )  
  
  
  # Put and Register Corsika File into SRM, DFC and LFC, in a coherent manner
  result = myAddFile(corsikaoutfileLFN, corsikafilename, 'CC-IN2P3-Disk')
  print "result of myAddFile for corsikaFile: ", result
  
  fileMetaData = fc.getFileMetadata(corsikaoutfileLFN)
  print "cosikaFile MetaData:",fileMetaData
  
###### insert File Level user metadata ############################################
# run number comes from this.runNumber
# number of triggered events?? 
# randomSeed DOES NOT come from logFile (this.executable.log), there are 3 seeds: seed1, seed2, seed3, for the moment, the seed comes from INPUT template, i.e. it is the same for every job????? 
# add jobID, coming from this.jobID


  #logFileKEYWORDS = ['SEED']
  #logfilename = executable + '.log'
  #dictlogFileKW = fileToKWDict(logfilename,logFileKEYWORDS)
  #print 'seed 1:',dictlogFileKW['SEED']
  

  corsikafilemd={}
  corsikafilemd['runNumber'] = int(run_number)
#  corsikafilemd['randomSeed'] = -1 ### fictif... to be defined
#  corsikafilemd['returncode'] = 0 ### to be defined

  result = fcc.setMetadata(corsikaoutfileLFN,corsikafilemd)
  print "result setMetadata=",result
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']
####################################################    
 
  corsika_tar = 'corsika_run' + run_number + '.tar.gz'
 
  cmdTuple = ['/bin/tar','zcfh',corsika_tar,rundir]
  ret = systemCall( 0, cmdTuple, sendOutput)
  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute tar')
    DIRAC.exit( -1 )


  fd = open('run_sim.sh', 'w' )
  fd.write( """#! /bin/sh                                                                                                                         
			echo "go for sim_telarray"
			. ./examples_common.sh
			zcat %s | $SIM_TELARRAY_PATH/run_sim_cta-ultra3""" % corsikafilename)
  fd.close()

  os.system('chmod u+x run_sim.sh')

  cmdTuple = ['./run_sim.sh']
  ret = systemCall( 0, cmdTuple, sendOutput)

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
    DIRAC.exit( -1 )
  
   
  simtelfilename = particle + '_' + zen + '_' + az + '_' + 'run' + run_number + '.simhess.gz' 
  cmd = 'mv Data/sim_telarray/cta-ultra3/0.0deg/Data/*.simtel.gz ' + simtelfilename 
  os.system(cmd)


  simtelparamTuple = [prodName,zen,az,particle,energyInfo,viewCone,simtelProdVersion,simtelfilename]
  simteloutfileLFN = "%s%s" % ( pathroot, '/'.join( simtelparamTuple ) )
  
  DIRAC.gLogger.notice( 'SimtelOutfile LFN is:', simteloutfileLFN )
  ret = myAddFile( simteloutfileLFN, simtelfilename, 'CC-IN2P3-Disk' )
  print "result of myAddFile for corsikaFile: ", ret
  
  
  ##########################################################
    
  fileMetaData = fc.getFileMetadata(simteloutfileLFN)
  print "fileMetaData:",fileMetaData
    
  ##########################################################
  
  
#if not result['OK']:
#  return self.__errorReport( 'Problem during DFC putAndRegister call', result['Message'] ) 

###### metadata ############################################

# no metadata for simtelarray file for the moment, only the ancestor... 

  simtelfilemd={}
# simtelfilemd['returncode'] = 0 ### to be defined
  
################# ancestor #############
  
  result = fcc.addFileAncestors({simteloutfileLFN:{'Ancestors': [ corsikaoutfileLFN ] }})
  print 'result addFileAncestor:', result

  result = fcc.setMetadata(simteloutfileLFN,simtelfilemd)
  if not result['OK']:
    print 'ResultSetMetadata:',result['Message']
    

  DIRAC.exit()


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



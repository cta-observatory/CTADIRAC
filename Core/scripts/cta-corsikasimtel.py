#!/usr/bin/env python
import DIRAC
import os

def sendSimtelOutput(stdid,line):
  logfilename = 'simtel.log'
  f = open( logfilename,'a')
  f.write(line)
  f.write('\n')
  f.close()
  
def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)
  
def main():

  from DIRAC.Core.Base import Script

  Script.registerSwitch( "T:", "template=", "Corsika Template" )
  Script.registerSwitch( "S:", "simtelConfig=", "SimtelConfig" )
  Script.registerSwitch( "p:", "run_number=", "Do not use: Run Number automatically set" )
  Script.registerSwitch( "E:", "executable=", "Executable (Use SetExecutable)")
  Script.registerSwitch( "V:", "version=", "Version (Use setVersion)")
  Script.registerSwitch( "M:", "mode=", "Mode (corsika_standalone/corsika_simtelarray)")


  Script.parseCommandLine( ignoreErrors = True )

  ## default values ##############
  run_number = None
  corsikaTemplate = None
  simtelConfig = None
  executable = None
  version = None
  mode = 'corsika_simtelarray'
  
  ### set switch values ###
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "run_number" or switch[0] == "p":
      run_number = switch[1].split('ParametricParameters=')[1]
    elif switch[0] == "template" or switch[0] == "T":
      corsikaTemplate = switch[1]
    elif switch[0] == "simtelConfig" or switch[0] == "S":
      simtelConfig = switch[1]
    elif switch[0] == "executable" or switch[0] == "E":
      executable = switch[1]
    elif switch[0] == "version" or switch[0] == "V":
      version = switch[1]
    elif switch[0] == "mode" or switch[0] == "M":
      mode = switch[1]
      
  
  if version == None or executable == None or run_number == None or corsikaTemplate == None:
    Script.showHelp()
    jobReport.setApplicationStatus('Missing options')
    DIRAC.exit( -1 )

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

  createGlobalsFromConfigFiles(corsikaTemplate,version)

  ############ Producing Corsika File

  install_CorsikaSimtelPack(version)
  CorsikaSimtelPack = 'corsika_simhessarray/' + version + '/corsika_simhessarray'
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

  if (mode == 'corsika_standalone'):
    DIRAC.exit()

############ Producing SimTel File
 ######################Building simtel Directory Metadata #######################

  cfg_dict = {"4MSST":'cta-prod2-4m-dc',"SCSST":'cta-prod2-sc-sst',"STD":'cta-prod2',"NSBX3":'cta-prod2',"ASTRI":'cta-prod2-astri',"NORTH":'cta-prod2n'}

  #if simtelConfig=="6INROW":
  #  all_configs=["4MSST","SCSST","ASTRI","NSBX3","STD","SCMST"]
  #elif simtelConfig=="5INROW":
  #  all_configs=["4MSST","SCSST","ASTRI","NSBX3","STD"]
  #else:
  #  all_configs=[simtelConfig]

  all_configs=[simtelConfig]

  for current_conf in all_configs:

    DIRAC.gLogger.notice('current conf is',current_conf)
    current_version = version
    DIRAC.gLogger.notice('current version is', current_version)

    #if current_conf == "SCMST":
    #  current_version = version + '_sc3'
    #  DIRAC.gLogger.notice('current version is', current_version)
    #  if os.path.isdir('sim_telarray'):
    #    DIRAC.gLogger.notice('Package found in the local area. Removing package...')
    #    cmd = 'rm -R sim_telarray corsika-6990 hessioxxx corsika-run'
    #    if(os.system(cmd)):
    #      DIRAC.exit( -1 )
    #    install_CorsikaSimtelPack(current_version)
    #else:
     # current_version = version
     # DIRAC.gLogger.notice('current version is', current_version)

#### execute simtelarray ################
    fd = open('run_sim.sh', 'w' )
    fd.write( """#! /bin/sh  
    export SVNPROD2=$PWD
    export SVNTAG=SVN-PROD2
    export CORSIKA_IO_BUFFER=800MB
    ./grid_prod2-repro.sh %s %s""" % (corsikaFileName,current_conf))
    fd.close()
####################################

    os.system('chmod u+x run_sim.sh')
    os.system('chmod u+x grid_prod2-repro.sh')
    cmdTuple = ['./run_sim.sh']
    ret = systemCall( 0, cmdTuple, sendSimtelOutput)
    simtelReturnCode, stdout, stderr = ret['Value']

 #   if(os.system('grep Broken simtel.log')==0):
 #     DIRAC.gLogger.error('Broken string found in simtel.log')
 #     jobReport.setApplicationStatus('Broken pipe')
 #     DIRAC.exit( -1 )

    if not ret['OK']:
      DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
      DIRAC.gLogger.error( 'run_sim.sh status is:', simtelReturnCode)
      DIRAC.exit( -1 )

##   check simtel data/log/histo Output File exist
    cfg = cfg_dict[current_conf]
    simtelFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.simtel.gz'
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Data/*.simtel.gz ' + simtelFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

    simtelLogFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.log.gz'
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Log/*.log.gz ' + simtelLogFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

    simtelHistFileName = particle + '_' + str(thetaP) + '_' + str(phiP) + '_alt' + str(obslev) + '_' + 'run' + run_number + '.hdata.gz'
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Histograms/*.hdata.gz ' + simtelHistFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

################################################################

  DIRAC.exit()

def install_CorsikaSimtelPack(version):

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
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
        cmd = 'cp -u -r ' + corsika_subdir + '/* .'       
        os.system(cmd)
        continue
    if workingArea:
      if checkSoftwarePackage( package, workingArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Local Area:', package )
        continue
      if installSoftwarePackage( package, workingArea() )['OK']:
      ############## compile #############################
        if version == 'prod-2_22072013_tox':
          DIRAC.gLogger.notice( 'Use already compiled version:', version )
          continue
        cmdTuple = ['./build_all','prod2','qgs2']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to execute build')
          DIRAC.exit( -1 )
        continue
    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  

def createGlobalsFromConfigFiles(corsikaConfigFileName,version):

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
  #prodKEYWORDS =  ['prodName','pathroot']
  #dictProdKW = fileToKWDict(prodConfigFileName,prodKEYWORDS)

  corsikaKEYWORDS = ['THETAP', 'PHIP', 'PRMPAR', 'ESLOPE' , 'ERANGE', 'VIEWCONE','NSHOW','TELFIL','OBSLEV','CSCAT']
  dictCorsikaKW = fileToKWDict(corsikaConfigFileName,corsikaKEYWORDS)

  #simtelKEYWORDS = ['env offset']

  # Formatting MD values retrieved in configFiles
  #prodName = dictProdKW['prodName'][0]
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
  
  #pathroot = dictProdKW['pathroot'][0]
  #building full prod, corsika and simtel Directories path
  #prodDirPath = os.path.join(pathroot,prodName)
  #corsikaDirPath = os.path.join(prodDirPath,corsikaProdVersion)
  #corsikaParticleDirPath = os.path.join(corsikaDirPath,particle)
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


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )




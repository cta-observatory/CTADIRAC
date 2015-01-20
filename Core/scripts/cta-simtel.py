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

  Script.registerSwitch( "p:", "inputfile=", "Input File" )
  Script.registerSwitch( "S:", "simtelConfig=", "SimtelConfig" )
  Script.registerSwitch( "V:", "version=", "Version (Use setVersion)")
  Script.registerSwitch( "C:", "comp=", "Compile (True/False)")

  Script.parseCommandLine( ignoreErrors = True )

  ## default values ##############
  simtelConfig = None
  version = None
  comp = True

  ### set switch values ###
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "inputfile" or switch[0] == "p":
      corsikaFileLFN = switch[1].split('ParametricInputData=LFN:')[1]
    elif switch[0] == "simtelConfig" or switch[0] == "S":
      simtelConfig = switch[1]
    elif switch[0] == "version" or switch[0] == "V":
      version = switch[1]    
    elif switch[0] == "comp" or switch[0] == "C":
      comp = switch[1]  
  
  if version == None:
    Script.showHelp()
    jobReport.setApplicationStatus('Missing options')
    DIRAC.exit( -1 )

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

############ Producing SimTel File
  if comp == True:
    install_CorsikaSimtelPack(version)
  else:
    CorsikaSimtelPack = 'corsika_simhessarray/' + version + '/corsika_simhessarray'
    res = installSoftwarePackage(  CorsikaSimtelPack, workingArea() )
    if not res['OK']:
      DIRAC.gLogger.error( 'Failed to execute installSoftwarePackage', res)

  CorsikaSimtelPack = 'corsika_simhessarray/' + version + '/corsika_simhessarray'

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
    ./grid_prod2-repro.sh %s %s""" % (os.path.basename(corsikaFileLFN),current_conf))
    fd.close()
####################################

    os.system('chmod u+x run_sim.sh')
    os.system('chmod u+x grid_prod2-repro.sh')
    cmdTuple = ['./run_sim.sh']
    ret = systemCall( 0, cmdTuple, sendSimtelOutput)
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
    simtelFileName = os.path.basename(corsikaFileLFN).replace('corsika.gz','simtel.gz')
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Data/*.simtel.gz ' + simtelFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

    simtelLogFileName = simtelFileName.replace('simtel.gz','log.gz')
    cmd = 'mv Data/sim_telarray/' + cfg + '/0.0deg/Log/*.log.gz ' + simtelLogFileName
    if(os.system(cmd)):
      DIRAC.exit( -1 )

    simtelHistFileName = simtelFileName.replace('simtel.gz','hdata.gz')
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
############### compile tox ################
          fd = open('run_compile.sh', 'w' )
          fd.write( """#! /bin/sh  
./build_all prod2 qgs2

# If the code was already there, we just clean but do not remove it.                                 
if [ -d "hessioxxx" ]; then
   (cd hessioxxx && make clean && make EXTRA_DEFINES='-DH_MAX_TEL=55 -DH_MAX_PIX=1984 -DH_MAX_SECTORS=13100 -DNO_LOW_GAIN')
fi

# If the code was already there, we just clean but do not remove it.                                 
if [ -d "sim_telarray" ]; then
   (cd sim_telarray && make clean && make EXTRA_DEFINES='-DH_MAX_TEL=55 -DH_MAX_PIX=1984 -DH_MAX_SECTORS=13100 -DNO_LOW_GAIN' install)
fi""")
          fd.close()
          os.system('chmod u+x run_compile.sh')
          cmdTuple = ['./run_compile.sh']
##########################################
        else:
          cmdTuple = ['./build_all','prod2','qgs2']

        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to execute build')
          DIRAC.exit( -1 )
        continue
    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

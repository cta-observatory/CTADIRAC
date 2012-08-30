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

def setMode( optionValue ):
  global mode
  mode = optionValue
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
  Script.registerSwitch( "M:", "mode=", "Mode", setMode )

  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

  jobID = os.environ['JOBID']
  jobID = int( jobID )
  jobReport = JobReport( jobID )

  Script.parseCommandLine( ignoreErrors = True )

  from CTADIRAC.Core.Workflow.Modules.CorsikaSimtelApp import CorsikaSimtelApp
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


  cs = CorsikaSimtelApp()

  cs.setSoftwarePackage(CorsikaSimtelPack)

  cs.csExe = executable

  cs.csArguments = ['--run-number',run_number,'--run',run,template] 

  res = cs.execute()

  if not res['OK']:
    DIRAC.gLogger.error( 'Failed to execute corsika_simtelarray Application')
    jobReport.setApplicationStatus('Corsika_simtelarray Application: Failed')
    DIRAC.exit( -1 )

  rundir = 'run' + run_number
    
  corsika_tar = 'corsika_run' + run_number + '.tar.gz'
 
  cmdTuple = ['/bin/tar','zcfh',corsika_tar,rundir]
  ret = systemCall( 0, cmdTuple, sendOutput)
  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute tar')
    DIRAC.exit( -1 )


  DIRAC.exit()


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



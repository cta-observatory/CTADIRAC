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

#  Script.registerSwitch( "R:", "run=", "Run" )
#  Script.registerSwitch( "P:", "config_path=", "Config Path" )
  Script.registerSwitch( "T:", "template=", "Corsika Template" )
  Script.registerSwitch( "S:", "simexe=", "Simtel Exe")
  Script.registerSwitch( "p:", "run_number=", "Do not use: Run Number automatically set" )
  Script.registerSwitch( "C:", "simconfig=", "Simtel Config (Optional)")
  Script.registerSwitch( "E:", "executable=", "Executable")
  Script.registerSwitch( "V:", "version=", "Version")


  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()

  if len( args ) < 1:
    Script.showHelp()
    
  ## default values ##############
  run_number == None
  template = None
  simexe = None
  simconfig = None
  executable = None
  version = None
  
  ### set switche values ###
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "run_number":
      run_number = switch[1].split('ParametricParameters=')[1]
    elif switch[0] == "template":
      template = switch[1]
    elif switch[0] == "simexe":
      simexe = switch[1]
    elif switch[0] == "simconfig":
      simconfig = switch[1]
    elif switch[0] == "executable":
      executable = switch[1]
    elif switch[0] == "version":
      version = switch[1]
  
  if version == None or executable == None or run_number == None or template == None or simexe == None:
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

### update the content of sim_telarray directory with personal config ##############
  if simconfig is not None:
    if(os.path.isdir(simconfig) == True):
      cmd = 'cp -r ' + simconfig + '/*' + ' sim_telarray'
      os.system(cmd)

  cs = CorsikaApp()
  cs.setSoftwarePackage(CorsikaSimtelPack)

###### execute corsika ###############
  cs.csExe = executable
  cs.csArguments = ['--run-number',run_number,'--run','corsika',template] 
  res = cs.execute()

  if not res['OK']:
    DIRAC.gLogger.error( 'Failed to execute corsika Application')
    jobReport.setApplicationStatus('Corsika Application: Failed')
    DIRAC.exit( -1 )

###### rename corsika file #################################
  rundir = 'run' + run_number
  corsikaKEYWORDS = ['TELFIL']
  dictCorsikaKW = fileToKWDict(template,corsikaKEYWORDS)
  corsikafilename = rundir + '/' + dictCorsikaKW['TELFIL'][0]
  destcorsikafilename = 'corsika_run' + run_number + '.corsika.gz'
  cmd = 'mv ' + corsikafilename + ' ' + destcorsikafilename
  os.system(cmd)
  
  ### create corsika tar ####################
  corsika_tar = 'corsika_run' + run_number + '.tar.gz'
  filetar1 = rundir + '/'+'input'
  filetar2 = rundir + '/'+ 'DAT' + run_number + '.dbase'
  filetar3 = rundir + '/run' + str(int(run_number)) + '.log'
  cmdTuple = ['/bin/tar','zcf',corsika_tar, filetar1,filetar2,filetar3]
  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  ret = systemCall( 0, cmdTuple, sendOutput)
  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute tar')
    DIRAC.exit( -1 )

###### execute sim_telarray ###############
  fd = open('run_sim.sh', 'w' )
  fd.write( """#! /bin/sh                                                                                                                         
			echo "go for sim_telarray"
			. ./examples_common.sh
			zcat %s | $SIM_TELARRAY_PATH/%s""" % (destcorsikafilename,simexe))
  fd.close()

  os.system('chmod u+x run_sim.sh')
  cmdTuple = ['./run_sim.sh']
  ret = systemCall( 0, cmdTuple, sendSimtelOutput)

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute run_sim.sh')
    DIRAC.exit( -1 )
    
  DIRAC.exit()

#### parse corsika template ##############
def fileToKWDict (fileName, keywordsList):    
  DIRAC.gLogger.notice('parsing: ', fileName)
  dict={}
  configFile = open(fileName, "r").readlines()
  for line in configFile:
    for word in line.split():
      if word in keywordsList:
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



#!/usr/bin/env python
import DIRAC
import os
import gzip

def setRunNumber( optionValue ):
  global run_number
  run_number = optionValue.split('ParametricParameters=')[1]
  return DIRAC.S_OK()
  
def setRun( optionValue ):
  global run
  run = optionValue
  return DIRAC.S_OK()

#def setConfigPath( optionValue ):
#  global config_path
#  config_path = optionValue
#  return DIRAC.S_OK()

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

def setSimExe( optionValue ):
  global simexe
  simexe = optionValue
  return DIRAC.S_OK()

def setConfig( optionValue ):
  global simconfig
  simconfig = optionValue
  return DIRAC.S_OK()


def sendSimtelOutput(stdid,line):
  logfilename = 'simtel.log.gz'
  f = gzip.open( logfilename,'ab')
  f.write(line)
  f.write('\n')
  f.close()
  
def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)
  
def main():

  from DIRAC.Core.Base import Script

  Script.registerSwitch( "p:", "run_number=", "Run Number", setRunNumber )
  Script.registerSwitch( "R:", "run=", "Run", setRun )
 # Script.registerSwitch( "P:", "config_path=", "Config Path", setConfigPath )
  Script.registerSwitch( "T:", "template=", "Corsika Template", setTemplate )
  Script.registerSwitch( "X:", "simexe=", "Simtel Exe", setSimExe )
  Script.registerSwitch( "S:", "simconfig=", "Simtel Config", setConfig )
  Script.registerSwitch( "E:", "executable=", "Executable", setExecutable )
  Script.registerSwitch( "V:", "version=", "Version", setVersion )
  Script.registerSwitch( "D:", "dcta=", "dcta")
  Script.registerSwitch( "I:", "icta=", "icta")
  Script.registerSwitch( "C:", "c_cta=", "c_cta")
  
  Script.parseCommandLine( ignoreErrors = False )

  import sys
  args = sys.argv[1:]

  from CTADIRAC.Core.Workflow.Modules.CorsikaApp import CorsikaApp
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
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
  if(os.path.isdir(simconfig) == True):
    cmd = 'cp -r ' + simconfig + '/*' + ' sim_telarray'
    os.system(cmd)

  cs = CorsikaApp()
  cs.setSoftwarePackage(CorsikaSimtelPack)

###### execute corsika ###############
  cs.csExe = executable
  cs.csArguments = ['--run-number',run_number,'--run',run,template] 
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
 # cmdTuple = ['/bin/tar','zcfh',corsika_tar,rundir]
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
  ret = getSoftwareEnviron( CorsikaSimtelPack )
  if not ret['OK']:
    error = ret['Message']
    DIRAC.gLogger.error( error, CorsikaSimtelPack )
    DIRAC.exit( -1 )

  corsikaEnviron = ret['Value']
  cmdTuple = [simexe]
  # add input file argument for sim_telarray  ###################
  inputfile = 'input_file='+destcorsikafilename
  inputfileopt = ['-C',inputfile]
  cmdTuple.extend(inputfileopt)
  # add output file argument for sim_telarray 
  destsimtelfilename = 'simtel_run' + run_number + '.simtel.gz'
  outputfile = 'output_file='+destsimtelfilename
  outputfileopt = ['-C',outputfile]
  cmdTuple.extend(outputfileopt)
  # add histo argument for sim_telarray 
  desthistofilename = 'simtel_run' + run_number + '.hdata.gz'
  histofile = 'histogram_file='+desthistofilename
  histofileopt = ['-C',histofile]
  cmdTuple.extend(histofileopt)

  # add other arguments for sim_telarray specified by user ######
  cmdTuple.extend(args[13:])
  ## remove 'parametric arguments' #############
  cmdTuple = cmdTuple[:-2]

  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  ret = systemCall( 0, cmdTuple, sendSimtelOutput,env = corsikaEnviron )

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute:',simexe)
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



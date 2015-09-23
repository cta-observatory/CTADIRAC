#!/usr/bin/env python
import DIRAC
import os
import sys
import glob
  
def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)
  
def main():

  from DIRAC.Core.Base import Script
  Script.initialize() 

  DIRAC.gLogger.notice('Platform is:')
  os.system('dirac-platform')
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from CTADIRAC.Core.Workflow.Modules.EvnDispApp import EvnDispApp
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from DIRAC.Core.Utilities.Subprocess import systemCall
  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

  jobID = os.environ['JOBID']
  jobID = int( jobID )
  jobReport = JobReport( jobID )

  version = sys.argv[3]
  DIRAC.gLogger.notice( 'Version:', version )

  EvnDispPack = os.path.join('evndisplay',version,'evndisplay')

  packs = [EvnDispPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    if checkSoftwarePackage( package, sharedArea() )['OK']:
      DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
      installSoftwareEnviron( package, sharedArea() )
      continue
    else:
      installSoftwarePackage( package, workingArea() )
      DIRAC.gLogger.notice( 'Package found in workingArea:', package )
      continue

    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  

  ed = EvnDispApp()
  ed.setSoftwarePackage(EvnDispPack)

  dstFileLFNList = sys.argv[-1].split( 'ParametricParameters={' )[1].split( '}' )[0].replace( ',', ' ' )

  args = []
  i = 0
  for word in dstFileLFNList.split():
    i = i + 1
    dstfile = os.path.basename( word )
###### execute evndisplay stage1 ###############
    executable = sys.argv[5]
    logfileName = executable + '_' + str( i ) + '.log'
    args = ['-sourcefile', dstfile, '-outputdirectory', 'outdir']
  # add other arguments for evndisp specified by user ######
    evndispparfile = open( 'evndisp.par', 'r' ).readlines()
    for line in evndispparfile:
      for word in line.split():
        args.append( word )

    execute_module( ed, executable, args )

    for name in glob.glob( 'outdir/*.root' ):
      evndispOutFile = name.split( '.root' )[0] + '_' + str( jobID ) + '_evndisp.root'
      cmd = 'mv ' + name + ' ' + os.path.basename( evndispOutFile )
      if( os.system( cmd ) ):
        DIRAC.exit( -1 )

########### quality check on Log #############################################
    cmd = 'mv ' + executable + '.log' + ' ' + logfileName
    if( os.system( cmd ) ):
      DIRAC.exit( -1 )
    fd = open( 'check_log.sh', 'w' )
    fd.write( """#! /bin/sh
if grep -i "error" %s; then
exit 1
fi
if grep "Final checks on result file (seems to be OK):" %s; then
exit 0
else
exit 1
fi
""" % (logfileName,logfileName))
    fd.close()

    os.system( 'chmod u+x check_log.sh' )
    cmd = './check_log.sh'
    DIRAC.gLogger.notice( 'Executing system call:', cmd )
    if( os.system( cmd ) ):
      jobReport.setApplicationStatus( 'EvnDisp Log Check Failed' )
      DIRAC.exit( -1 )
##################################################################
########### remove the dst file #############################################
    cmd = 'rm ' + dstfile
    if( os.system( cmd ) ):
      DIRAC.exit( -1 )
 
  DIRAC.exit()

def execute_module(ed,executable,args):

  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

  jobID = os.environ['JOBID']
  jobID = int( jobID )
  jobReport = JobReport( jobID )

  DIRAC.gLogger.notice( 'Executable:',executable )
  DIRAC.gLogger.notice( 'Arguments:', args )
  ed.edExe = executable
  ed.edArguments = args  
  evnDisplayReturnCode = ed.execute()
  
  if evnDisplayReturnCode != 0:
    DIRAC.gLogger.error( 'Failed to execute EvnDisp Application')
    jobReport.setApplicationStatus('EvnDisp Application: Failed')
    DIRAC.exit( -1 )

  return DIRAC.S_OK

if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



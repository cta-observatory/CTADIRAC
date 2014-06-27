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
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
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
      cmd = 'cp -r ' + os.path.join(sharedArea(),'evndisplay',version,'EVNDISP.CTA.runparameter') + ' .'
      if(os.system(cmd)):
        DIRAC.exit( -1 )
      cmd = 'cp -r ' + os.path.join(sharedArea(),'evndisplay',version,'Calibration') + ' .'
      if(os.system(cmd)):
        DIRAC.exit( -1 )
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

########## Use of trg mask file #######################
  usetrgfile = sys.argv[7]
  DIRAC.gLogger.notice( 'Usetrgfile:', usetrgfile )

####### Use of multiple inputs per job ###
  simtelFileLFNList = sys.argv[-1].split('ParametricParameters={')[1].split('}')[0].replace(',',' ')
  # first element of the list
  simtelFileLFN = simtelFileLFNList.split(' ')[0]  
  ## convert the string into a list and get the basename
  simtelFileList = []
  for word in simtelFileLFNList.split():
    simtelFileList.append(os.path.basename(word))

####  Parse the Layout List #################
  layoutList = parseLayoutList(sys.argv[9])
#############################################

####  Loop over the Layout List #################
  for layout in layoutList: 
    args = []
########## download trg mask file #######################
    if usetrgfile == 'True':
      trgmaskFileLFN=simtelFileLFN.replace('simtel.gz','trgmask.gz')
      DIRAC.gLogger.notice( 'Trying to download the trgmask File', trgmaskFileLFN)
      result = ReplicaManager().getFile( trgmaskFileLFN)
      if not result['OK']:
        DIRAC.gLogger.error( 'Failed to download trgmakfile:', result )
        jobReport.setApplicationStatus('Trgmakfile download Error')
        DIRAC.exit( -1 )    
      args.extend(['-t',os.path.basename(trgmaskFileLFN)])
############################################################
###### execute evndisplay converter ##################
    executable = sys.argv[5]
    dstfile = layout + '_' + os.path.basename(simtelFileLFN).replace('simtel.gz','dst.root')
    logfileName =  executable + '_' + layout + '.log'
    layout = os.path.join('EVNDISP.CTA.runparameter/DetectorGeometry',layout)
    DIRAC.gLogger.notice( 'Layout is:', layout)

  # add other arguments for evndisplay converter specified by user ######
    converterparfile = open('converter.par', 'r').readlines()  
    for line in converterparfile:
      for word in line.split():
        args.append(word) 
#########################################################
    args.extend(['-a',layout])
    args.extend(['-o',dstfile])
    args.extend(simtelFileList)
    execute_module(ed,executable,args)
########### check existence of DST file ###############
    if not os.path.isfile(dstfile):
      DIRAC.gLogger.error( 'DST file Missing:', dstfile )
      jobReport.setApplicationStatus('DST file Missing')
      DIRAC.exit( -1 )

########### quality check on Log #############################################
    cmd = 'mv ' + executable + '.log' + ' ' + logfileName 
    if(os.system(cmd)):
      DIRAC.exit( -1 )

    fd = open('check_log.sh', 'w' )
    fd.write( """#! /bin/sh  
MCevts=$(grep writing  %s | grep "MC events" | awk '{print $2}')
if [ $MCevts -gt 0 ]; then
    exit 0
else
    echo "MCevts is zero"
    exit -1
fi
""" % (logfileName))
    fd.close()

    os.system('chmod u+x check_log.sh')
    cmd = './check_log.sh'
    DIRAC.gLogger.notice( 'Executing system call:', cmd )
    if(os.system(cmd)):
      jobReport.setApplicationStatus('Converter Log Check Failed')
      DIRAC.exit( -1 )

###### execute evndisplay stage1 ###############
    executable = 'evndisp'
    logfileName =  executable + '_' + os.path.basename(layout) + '.log'

    args = ['-sourcefile',dstfile,'-outputdirectory','outdir']
  # add other arguments for evndisp specified by user ######
    evndispparfile = open('evndisp.par', 'r').readlines()  
    for line in evndispparfile:
      for word in line.split():
        args.append(word) 

    execute_module(ed,executable,args)

    for name in glob.glob('outdir/*.root'):
      evndispOutFile = name.split('.root')[0] + '_' + os.path.basename(layout) + '_evndisp.root'
      cmd = 'mv ' +  name + ' ' + os.path.basename(evndispOutFile)
      if(os.system(cmd)):
        DIRAC.exit( -1 )

########### quality check on Log #############################################
    cmd = 'mv ' + executable + '.log' + ' ' + logfileName 
    if(os.system(cmd)):
      DIRAC.exit( -1 )
    fd = open('check_log.sh', 'w' )
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

    os.system('chmod u+x check_log.sh')
    cmd = './check_log.sh'
    DIRAC.gLogger.notice( 'Executing system call:', cmd )
    if(os.system(cmd)):
      jobReport.setApplicationStatus('EvnDisp Log Check Failed')
      DIRAC.exit( -1 )
##################################################################
 
  DIRAC.exit()

def parseLayoutList(layoutlist):

  f = open(layoutlist,'r')

  layoutList = []
  for line in f:
    layout = line.strip()
    if line!="\n":
      layoutList.append(layout)

  return layoutList

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



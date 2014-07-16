#!/usr/bin/env python
import DIRAC
import os
import sys
  
def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)
  
def main():

  from DIRAC.Core.Base import Script
  Script.initialize() 

  DIRAC.gLogger.notice('Platform is:')
  os.system('dirac-platform')
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  from CTADIRAC.Core.Workflow.Modules.Read_CtaApp import Read_CtaApp
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
  install_CorsikaSimtelPack(version)

  ######### run read_cta #######################################

  rcta = Read_CtaApp()
  CorsikaSimtelPack = os.path.join('corsika_simhessarray',version,'corsika_simhessarray')
  rcta.setSoftwarePackage(CorsikaSimtelPack)
  rcta.rctaExe = 'read_cta'

  # add arguments for read_cta specified by user ######
  args = []
  rctaparfile = open('read_cta.par', 'r').readlines()  
  for line in rctaparfile:
    for word in line.split():
      args.append(word) 

  simtelFileLFN = sys.argv[-1].split('ParametricInputData=LFN:')[1]
  simtelFileName = os.path.basename(simtelFileLFN)
  dstFileName = simtelFileName.replace('simtel.gz','simtel-dst.gz')
  dstHistoFileName = simtelFileName.replace('simtel.gz','hdata-dst.gz')

  args.extend(['--dst-file', dstFileName, '--histogram-file', dstHistoFileName, simtelFileName])
  rcta.rctaArguments = args

  rctaReturnCode = rcta.execute()
  
  if rctaReturnCode != 0:
    DIRAC.gLogger.error( 'read_cta Application: Failed')
    jobReport.setApplicationStatus('read_cta Application: Failed')
    DIRAC.exit( -1 )
#################################################################
  from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
  ret = getSoftwareEnviron( CorsikaSimtelPack )

  if not ret['OK']:
    error = ret['Message']
    DIRAC.gLogger.error( error, CorsikaSimtelPack )
    DIRAC.exit( -1 )

  read_ctaEnviron = ret['Value']

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
  ret = systemCall( 0, cmdTuple, sendOutput,env = read_ctaEnviron)
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

if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



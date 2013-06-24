#!/usr/bin/env python

import DIRAC
import os


def main():

  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  Script.parseCommandLine(ignoreErrors = True)

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from DIRAC.Core.Utilities.Subprocess import systemCall


  args = Script.getPositionalArgs()
  version = args[0]
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
        if version == 'clean_23012012':
          cmdTuple = ['./build_all','ultra','qgs2']
        elif version in ['prod-2_21122012','prod-2_08032013','prod-2_06052013']:
          cmdTuple = ['./build_all','prod2','qgs2']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
          DIRAC.gLogger.error( 'Failed to compile')
          DIRAC.exit( -1 )
        continue

    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 ) 

  ret = getSoftwareEnviron( CorsikaSimtelPack )
  if not ret['OK']:
    error = ret['Message']
    DIRAC.gLogger.error( error, CorsikaSimtelPack )
    DIRAC.exit( -1 )

  corsikaEnviron = ret['Value']

  executable_file = args[1]
  cmd = 'chmod u+x ' + executable_file
  os.system(cmd)

  cmdTuple = args[1:] 

  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  ret = systemCall( 0, cmdTuple, sendOutput, env = corsikaEnviron )

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute read_hess:', ret['Message'] )
    DIRAC.exit( -1 )

  status, stdout, stderr = ret['Value']
  if status:
    DIRAC.gLogger.error( 'read_hess execution reports Error:', status )
    DIRAC.gLogger.error( stdout )
    DIRAC.gLogger.error( stderr )
    DIRAC.exit( -1 )
    
  DIRAC.exit()

def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



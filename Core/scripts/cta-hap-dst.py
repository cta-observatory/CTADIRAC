#!/usr/bin/env python 
import DIRAC
import os

def main():

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  from CTADIRAC.Core.Workflow.Modules.HapDST import HapDST

  DIRAC.gLogger.notice( 'Executing a make_CTA_DST.C Root Macro' )

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea

  hd = HapDST()

  packs = ['HESS/v0.1/lib','HESS/v0.1/root','HAP/v0.3/HAP']

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        continue
    if localArea:
      if checkSoftwarePackage( package, localArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Local Area:', package )
        continue
      if installSoftwarePackage( package, localArea() )['OK']:
        continue
    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    return DIRAC.S_ERROR( '%s not available' % package )

  hd.setSoftwarePackage('HAP/v0.3/HAP')

  
  args = Script.getPositionalArgs()
  DIRAC.gLogger.notice( 'Arguments:', args )

  # There is a bug in the Job.py class that produce a duplicated is the first argument
  if args[1].find( args[0] ) == 0:
    hd.rootMacro = args[1]
    hd.rootArguments = args[2:]
  else:
    hd.rootMacro = args[1]
    hd.rootArguments = args[2:]


  res = hd.execute()

  if not res['OK']:
    DIRAC.exit( -1 )

  DIRAC.exit()




if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

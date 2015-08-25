#!/usr/bin/env python

import DIRAC
import os

def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

def main():

  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  from SoftwareInstallation import checkSoftwarePackage
  from SoftwareInstallation import installSoftwarePackage
  from SoftwareInstallation import installSoftwareEnviron
  from SoftwareInstallation import localArea
  from SoftwareInstallation import sharedArea
  from SoftwareInstallation import workingArea
  from SoftwareInstallation import createSharedArea
  from DIRAC.Core.Utilities.Subprocess import systemCall

  DIRAC.gLogger.notice('Platform is:')

  os.system('dirac-platform')
  
  args = Script.getPositionalArgs()
  version = args[0]
  
  area = sharedArea()
  

  if area:
    gLogger.notice( 'Using Shared Area at:', area)    
   # if defined, check that it really exists
    if not os.path.isdir( area ):
      gLogger.error( 'Missing Shared Area Directory:', area )
      if createSharedArea() == True:
        gLogger.notice( 'Shared Area created')
      else:
        gLogger.error( 'Failed to create Shared Area Directory:', area )
        DIRAC.exit ( -1 )
  else:
    if createSharedArea() == True:
      gLogger.notice( 'Shared Area created')
    else:
      gLogger.error( 'Failed to create Shared Area Directory:', area )
      DIRAC.exit ( -1 )

  ctoolspackage = os.path.join('ctools',version,'ctools')

  packs = [ctoolspackage]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    packageTuple = package.split( '/' )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        continue
      if installSoftwarePackage( package, sharedArea() )['OK']:
        continue

    DIRAC.gLogger.error( 'Software package not correctly installed')
    DIRAC.exit( -1 )  

  DIRAC.exit()


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )


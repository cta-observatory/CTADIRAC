#!/usr/bin/env python

import DIRAC
import os

def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

def main():

  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import workingArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import createSharedArea
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


  EvnDispSimtelPack = os.path.join('evndisplay', version, 'evndisplay')

  packs = [EvnDispSimtelPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    packageTuple = package.split( '/' )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        continue
      if installSoftwarePackage( package, sharedArea() )['OK']:

        if not os.path.isdir( os.path.join(sharedArea(),packageTuple[0], packageTuple[1])):
          DIRAC.gLogger.error( 'Software package missing in the shared area')
          DIRAC.exit( -1 )

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


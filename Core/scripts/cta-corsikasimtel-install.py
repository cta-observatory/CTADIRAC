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
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import createSharedArea
  from DIRAC.Core.Utilities.Subprocess import systemCall

  args = Script.getPositionalArgs()
  version = args[0]
  
  area = sharedArea()


  if area:
    gLogger.notice( 'Using Shared Area at:', area)    
  else:
    if createSharedArea() == True:
      gLogger.notice( 'Shared Area created:', sharedArea())
      if (os.mkdir( os.path.join(sharedArea(),'corsika_simhessarray'))):
        gLogger.error( 'Failed to create corsika_simhessarray Directory')
        DIRAC.exit( -1 )
      gLogger.notice( 'corsika_simhessarray Directory created')
    else:
      gLogger.error( 'Failed to create Shared Area Directory')
      DIRAC.exit ( -1 )

  CorsikaSimtelPack = os.path.join('corsika_simhessarray', version, 'corsika_simhessarray')

  packs = [CorsikaSimtelPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    packageTuple = package.split( '/' )
#    if sharedArea:
    if checkSoftwarePackage( package, sharedArea() )['OK']:
      DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
      continue
#      if installSoftwarePackage( package, sharedArea() )['OK']:
      ############## compile #############################
    installdir = os.path.join( sharedArea(), packageTuple[0], packageTuple[1])
    fd = open('run_compile.sh', 'w' )
    fd.write( """#!/bin/sh      
current_dir=${PWD}
package=%s
installdir=%s
if ! [ -d ${package} ]; then
mkdir ${package}
fi
mkdir ${installdir} 
cd ${installdir} 
mkdir sim sim-sc3
(cd sim && tar zxvf ${current_dir}/corsika_simhessarray.tar.gz && ./build_all prod2 qgs2)
(cd sim-sc3 && tar zxvf ${current_dir}/corsika_simhessarray.tar.gz && ./build_all sc3 qgs2)""" % (packageTuple[0],installdir))
    fd.close()
    os.system('chmod u+x run_compile.sh')
    if(os.system('./run_compile.sh')):
      DIRAC.gLogger.error( 'Failed to compile')
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



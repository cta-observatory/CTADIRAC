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

  CorsikaSimtelPack = 'corsika_simhessarray/' + version + '/corsika_simhessarray'

  packs = [CorsikaSimtelPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    packageTuple = package.split( '/' )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        continue
      if installSoftwarePackage( package, sharedArea() )['OK']:
      ############## compile #############################
        installdir = os.path.join( sharedArea(), packageTuple[0], packageTuple[1])
        cmd = 'cp -R CODE/ ' + installdir
        os.system(cmd)
        fd = open('run_compile.sh', 'w' )
        fd.write( """#! /bin/sh                                                                                                                         
cd %s
./build_all prod2 qgs
./CODE/build_simtel prod2 qgs2 build_prod2_qgs2
./CODE/build_simtel sc2_111 qgs2 build_sc2_111_qgs2""" % (installdir))

        fd.close()
        os.system('chmod u+x run_compile.sh')
        cmdTuple = ['./run_compile.sh']
        ret = systemCall( 0, cmdTuple, sendOutput)
        if not ret['OK']:
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


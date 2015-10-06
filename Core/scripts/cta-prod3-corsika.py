#!/usr/bin/env python

import os

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Subprocess import systemCall

def setRunNumber( optionValue ):
  global run_number
  run_number = optionValue.split('ParametricParameters=')[1]
  return DIRAC.S_OK()

def sendOutput( stdid, line ):
  f = open( 'Corsika_Log.txt', 'a' )
  f.write( line )
  f.write( '\n' )
  f.close()

def main():
  Script.registerSwitch( "p:", "run_number=", "Run Number", setRunNumber )
  Script.parseCommandLine( ignoreErrors = True )
  from CTADIRAC.Core.Utilities.Prod3SoftwareManager import Prod3SoftwareManager
  args = Script.getPositionalArgs()

  # get arguments
  package = args[1]
  version = args[2]
  arch = "sl6-gcc44"
  input_card = args[3]

  # # install software
  prod3swm = Prod3SoftwareManager()
  # check where package is installed
  res = prod3swm.checkSoftwarePackage( package, version, arch )
  if not res['OK']:
    res = prod3swm.installSoftwarePackage( package, version, arch )
    if not res['OK']:
      return res
    else:
      package_dir = res['Value']
      prod3swm.dumpSetupScriptPath( package_dir )
  else:
  # # dump the SetupScriptPath to be sourced by DIRAC scripts
  # ## copy DIRAC scripts in the current directory
    package_dir = res['Value']
    prod3swm.dumpSetupScriptPath( package_dir )
    res = prod3swm.installDIRACScripts( package_dir )
    if not res['OK']:
      return res

  # # run corsika
  cmdTuple = ['./dirac_prod3_corsika_only', '--run', run_number, input_card]
  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  res = systemCall( 0, cmdTuple, sendOutput )
  if not res['OK']:
    return res

  # ## rename output file
  outfile = 'run%s.corsika.gz' % run_number
  cmd = 'mv Data/corsika/*/*corsika.gz %s' % outfile
  if( os.system( cmd ) ):
    DIRAC.exit( -1 )

  return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
  try:
    res = main()
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

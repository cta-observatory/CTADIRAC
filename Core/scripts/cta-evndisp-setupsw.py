#!/usr/bin/env python
""" EvnDisp Script to setup software
"""

__RCSID__ = "$Id$"

# generic imports
import os, tarfile

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script


Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s package version (arch)' % Script.scriptName,
                                     'Arguments:',
                                     '  package: evndisplay',
                                     '  version: 2015-xx-xx',
                                     '\ne.g: %s evndisplay 2015-xx-xx' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

# Specific DIRAC imports
from CTADIRAC.Core.Utilities.Prod3SoftwareManager import Prod3SoftwareManager

def setupSoftware( args ):
  """ setup a given software package
        to be used in main
    Keyword arguments:
    args -- a list of arguments in order [package, version, arch]
  """
  # check number of arguments
  if len( args ) not in [2, 3]:
    Script.gLogger.notice()
    Script.showHelp()
    return DIRAC.S_ERROR( 'Wrong number of arguments' )

  # get arguments
  package = args[0]
  version = args[1]
  arch = "sl6-gcc44"
  if len( args ) == 3:
    arch = args[2]

  prod3swm = Prod3SoftwareManager( soft_category = {"evndisplay":"simulations"} )
  # check if Package is installed
  res = prod3swm.checkSoftwarePackage( package, version, arch )
  if not res['OK']:
    res = prod3swm.installSoftwarePackage( package, version, arch )
    if not res['OK']:
      return res
    else:
      package_dir = res['Value']
      prod3swm.dumpSetupScriptPath( package_dir )
      return DIRAC.S_OK()

  # # dump the SetupScriptPath to be sourced by DIRAC scripts
  # ## copy DIRAC scripts in the current directory
  package_dir = res['Value']
  prod3swm.dumpSetupScriptPath( package_dir )
  res = prod3swm.installDIRACScripts( package_dir )
  if not res['OK']:
    return res

  return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
  args = Script.getPositionalArgs()
  try:
    res = setupSoftware( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Setup software completed successfully' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

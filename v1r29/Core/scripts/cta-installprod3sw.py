#!/usr/bin/env python
""" Prod3 MC Script to install software
"""
import os
# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script


Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s package version (arch)' % Script.scriptName,
                                     'Arguments:',
                                     '  package: corsika_simhessarray',
                                     '  version: 2015-07-13',
                                     '\ne.g: %s corsika_simhessarray 2015-06-02' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

# Specific DIRAC imports
from CTADIRAC.Core.Utilities.Prod3SoftwareManager import Prod3SoftwareManager

def installSoftwareSharedArea( args ):
  """ install a given software package
        to be used in main
    Keyword arguments:
    args -- a list of arguments in order [package, version, arch]
  """
  # get arguments
  package = args[0]
  version = args[1]
  arch = "sl6-gcc44"
  if len( args ) == 3:
    arch = args[2]
    
  prod3swm = Prod3SoftwareManager()
  res = prod3swm._getSharedArea()
  if not res['OK']:
    return res
  area = res['Value']
  installDir = prod3swm._getPackageDir ( area, arch, package, version )
  # check if Package is installed
  res = prod3swm.checkSoftwarePackage( package, version, arch, area = 'SW_SHARED_DIR' )

  if not res['OK']:
    res = prod3swm.installSoftwarePackage( package, version, arch, installDir )
    if not res['OK']:
      return res

  return DIRAC.S_OK()

####################################################
if __name__ == '__main__':

  DIRAC.gLogger.setLevel( 'VERBOSE' )
  args = Script.getPositionalArgs()
  try:
    # check number of arguments
    if len( args ) not in [2, 3]:
      Script.showHelp()
    res = installSoftwareSharedArea( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Install software completed successfully' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

#!/usr/bin/env python
"""
  Replicate a given version of the corsika_simhessarray sw to a list of SE or All default SE
"""

__RCSID__ = "$Id$"

import os, signal
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... version Dest' % Script.scriptName,
                                     'Arguments:',
                                     '  corsika_simhessarray version:      e.g. 2015-08-18',
                                     '  Dest SE:     e.g. CC-IN2P3-Disk DESY-ZN-Disk/All'] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac import Dirac

def replicateFile( lfn, seName, printOutput = True ):
  dirac = Dirac()
  result = dirac.replicateFile( lfn, seName, printOutput = True )
  return result

# Register an handler for the timeout
def handler( signum, frame ):
  raise Exception( "Timeout exceeded!" )

def replicatesw( args ):
  version = args[0]
  seNameList = args[1:]

  # # Default list
  if args[1] == 'All':
      seNameList = ['CC-IN2P3-Disk', 'CYF-STORM-Disk', 'DESY-ZN-Disk', 'CEA-Disk', 'LPNHE-Disk', 'LAPP-Disk', 'CNAF-Disk', 'PIC-Disk', 'CIEMAT-Disk', 'M3PEC-Disk']

  lfn = os.path.join( '/vo.cta.in2p3.fr/software/corsika_simhessarray/', version, 'corsika_simhessarray.tar.gz' )

  finalResult = {"Failed":[], "Successful":[]}

  for seName in seNameList:
    DIRAC.gLogger.notice( 'Replicating sw to', seName )
    try:
      result = replicateFile( lfn, seName, printOutput = True )
      if not result['OK']:
        finalResult["Failed"].append( lfn )
        print 'ERROR %s' % ( result['Message'] )
      else:
        finalResult["Successful"].append( lfn )
    except Exception, exc:
      print exc

  print finalResult
  return DIRAC.S_OK()

  ####################################################
if __name__ == '__main__':
  args = Script.getPositionalArgs()
  if len( args ) < 2:
    Script.showHelp()
  signal.signal( signal.SIGALRM, handler )
  timeout = 5  # in seconds
  signal.alarm( timeout )
  try:
    res = replicatesw( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )









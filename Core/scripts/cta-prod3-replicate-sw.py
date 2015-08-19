#!/usr/bin/env python
"""
  Replicate a given version of the corsika_simhessarray sw to a list of SE or All default SE
"""

__RCSID__ = "$Id$"

import os
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... version Dest' % Script.scriptName,
                                     'Arguments:',
                                     '  corsika_simhessarray version:      e.g. 2015-08-18',
                                     '  Dest SE:     e.g. CC-IN2P3-Disk DESY-ZN-Disk/All'] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 2:
  Script.showHelp()

version = args[0]
seNameList = args[1:]

# # Default list
if args[1] == 'All':
    seNameList = ['CC-IN2P3-Disk', 'CYF-STORM-Disk', 'DESY-ZN-Disk', 'CEA-Disk', 'LPNHE-Disk', 'LAPP-Disk', 'CNAF-Disk', 'PIC-Disk', 'CIEMAT-Disk', 'M3PEC-Disk']

lfn = os.path.join( '/vo.cta.in2p3.fr/software/corsika_simhessarray/', version, 'corsika_simhessarray.tar.gz' )

from DIRAC.Interfaces.API.Dirac                       import Dirac
dirac = Dirac()
exitCode = 0

finalResult = {"Failed":[], "Successful":[]}

for seName in seNameList:
  DIRAC.gLogger.notice( 'Replicating sw to', seName )
  result = dirac.replicateFile( lfn, seName, printOutput = True )
  if not result['OK']:
    finalResult["Failed"].append( lfn )
    print 'ERROR %s' % ( result['Message'] )
    exitCode = 2
  else:
    finalResult["Successful"].append( lfn )

print finalResult

DIRAC.exit( exitCode )

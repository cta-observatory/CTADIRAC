#!/usr/bin/env python

"""
  Delete an existing Transformation
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s TransID infile' % Script.scriptName,
                                     'Arguments:',
                                     '  TransID: Transformation ID',
                                     '\ne.g: %s 381' % Script.scriptName,
                                     ] ) )


Script.parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()
if ( len( args ) != 1 ):
  Script.showHelp()

# get arguments
TransID = args[0]

tc = TransformationClient()
res = tc.deleteTransformation( TransID )

if not res['OK']:
  DIRAC.gLogger.error ( res['Message'] )
  DIRAC.exit( -1 )
else:
  DIRAC.exit( 0 )



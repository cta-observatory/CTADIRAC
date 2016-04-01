#!/usr/bin/env python

"""
  Add files to an existing Transformation
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s TransID infile' % Script.scriptName,
                                     'Arguments:',
                                     '  infile: ascii file with LFNs',
                                     '  TransID: Transformation ID',
                                     '\ne.g: %s 381 Paranal_gamma_North.list' % Script.scriptName,
                                     ] ) )


Script.parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()
if ( len( args ) != 2 ):
  Script.showHelp()

# get arguments
TransID = args[0]
infile = args[1]
f = open( infile, 'r' )

infileList = []
for line in f:
  infile = line.strip()
  if line != "\n":
    infileList.append( infile )

tc = TransformationClient()
res = tc.addFilesToTransformation( TransID , infileList )  # Files added here

if not res['OK']:
  DIRAC.gLogger.error ( res['Message'] )
  DIRAC.exit( -1 )
else:
  DIRAC.exit( 0 )



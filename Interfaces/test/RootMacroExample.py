#!/usr/bin/env python
"""
  Submit an Example RootMacro Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [Site] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Requested Site' ] ) )
Script.parseCommandLine()


def RootExample( destination = None ) :
  from CTADIRAC.Interfaces.API.RootJob import RootJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = RootJob( './RootExample.C', ["test", 5000] )

  if destination:
    j.setDestination( destination )

  j.setOutputSandbox( [ 'histo.ps' ] )

  Script.gLogger.info( j._toJDL() )

  return Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  ret = RootExample( args )
  if ret['OK']:
    Script.gLogger.notice( 'Submitted Job:', ret['Value'] )
  else:
    Script.gLogger.error( ret['Message'] )

#!/usr/bin/env python
"""
  Submit a Software Installation Job
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [version]' % Script.scriptName,
                                     'Arguments:',
                                     '  version: prod-2_21122012/prod-2_08032013',
                                     '  site: Site Destination'] ) )

Script.parseCommandLine()

def softinstall( args = None ) :

  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.Job import Job

  if (len(args)!=2):
    Script.showHelp()

  version = args[0]
  site = args[1]

  if version not in ['prod-2_21122012','prod-2_08032013']:
    Script.gLogger.error('Version not valid')
    Script.showHelp()

  j = Job()

  j.setInputSandbox( ['cta-swinstall.py','SoftwareInstallation.py','CODE'] )   

  j.setExecutable('./cta-swinstall.py', version)

  j.setDestination([site])

  j.setName('SoftInstall')

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )

  Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    softinstall( args )
  except Exception:
    Script.gLogger.exception()

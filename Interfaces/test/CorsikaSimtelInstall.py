#!/usr/bin/env python
"""
  Submit a Software Installation Job
"""
import os
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [version]' % Script.scriptName,
                                     'Arguments:',
                                     '  version: prod-2_13112014',
                                     '  site: Site Destination'] ) )

Script.parseCommandLine()

def CorsikaSimtelInstall( args = None ) :

  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.Job import Job

  if (len(args)!=2):
    Script.showHelp()

  version = args[0]
  site = args[1]

  if version not in ['prod-2_13112014']:
    Script.gLogger.error('Version not valid')
    Script.showHelp()

  j = Job()
  CorsikaSimtelPack = os.path.join('corsika_simhessarray', version, 'corsika_simhessarray')
  CorsikaSimtelLFN = 'LFN:' + os.path.join( '/vo.cta.in2p3.fr/software', CorsikaSimtelPack) + '.tar.gz'
  j.setInputSandbox( ['cta-corsikasimtel-install.py',CorsikaSimtelLFN] )   
  j.setExecutable('./cta-corsikasimtel-install.py', version)
  j.setDestination([site])
  j.setName('corsikasimtelInstall')
  j.setCPUTime(100000)
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelInstall( args )
  except Exception:
    Script.gLogger.exception()

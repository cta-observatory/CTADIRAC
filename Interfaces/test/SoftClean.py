#!/usr/bin/env python
"""
  Submit a Software Clean Job
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [version]' % Script.scriptName,
                                     'Arguments:',
                                     '  package: corsika_simhessarray/evndisplay/HESS/ctools',
                                     '  version: prod-2_21122012/prod-2_08032013/prod-2_06052013/prod-2_06052013_sc3/prod2_130708/prod2_130718/v0.1',
                                     '  site: Site Destination'] ) )

Script.parseCommandLine()

def SoftClean( args = None ) :

  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.Job import Job

  if (len(args)!=3):
    Script.showHelp()

  package = args[0]
  version = args[1]
  site = args[2]

  j = Job()

  j.setInputSandbox( ['cta-swclean.py'] )   

  arguments = package + ' ' + version
  j.setExecutable('./cta-swclean.py', arguments)

  j.setDestination([site])

  name = 'SoftClean_' + package + '_' + version
  j.setName('SoftClean')
  j.setJobGroup('SoftClean')

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )

  res = Dirac().submit( j )
  
  print res['Value']
  
if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    SoftClean( args )
  except Exception:
    Script.gLogger.exception()



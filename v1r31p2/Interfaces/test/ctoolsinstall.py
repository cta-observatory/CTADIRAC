#!/usr/bin/env python
"""
  Submit a Software Installation Job
"""

from DIRAC.Core.Base import Script
import DIRAC

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [version]' % Script.scriptName,
                                     'Arguments:',
                                     '  version: 18122013/v0r7p0',
                                     '  site: Site Destination',
                                     '  ce: CE Destination (optional)'] ) )



Script.parseCommandLine()

def softinstall( args = None ) :

  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.Job import Job

  if (len(args)<2):
    Script.showHelp()

  version = args[0]
  site = args[1]
 
  if version not in ['18122013','v0r7p0']:
    Script.gLogger.error('Version not valid')
    Script.showHelp()

  j = Job()
 
  j.setInputSandbox( ['cta-ctools-install.py','SoftwareInstallation.py'] )   

  j.setExecutable('./cta-ctools-install.py', version)

  j.setDestination([site])

  j.setName('ctoolsInstall')

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )

  if site in ['LCG.GRIF.fr','LCG.M3PEC.fr']:
    if site == 'LCG.GRIF.fr':
      ceList = ['apcce02.in2p3.fr','grid36.lal.in2p3.fr','lpnhe-cream.in2p3.fr','llrcream.in2p3.fr','node74.datagrid.cea.fr']
    elif site == 'LCG.M3PEC.fr':
#      ceList = ['ce0.bordeaux.inra.fr','ce0.m3pec.u-bordeaux1.fr']
      ceList = ['ce0.bordeaux.inra.fr']
    for ce in ceList:
      j.setDestinationCE(ce)
      name = 'ctoolsInstall' + '_' + ce
      j.setName(name)
      res = Dirac().submit( j )
      print res
    DIRAC.exit()
  else:
    name = 'ctoolsInstall'   

  j.setName(name)
  res = Dirac().submit( j )
  print res

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    softinstall( args )
  except Exception:
    Script.gLogger.exception()

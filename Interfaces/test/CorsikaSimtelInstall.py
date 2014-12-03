#!/usr/bin/env python
"""
  Submit a CorsikaSimtel Software Installation Job
"""
import os
import DIRAC
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
  j.setJobGroup('SoftInstall')
  j.setCPUTime(100000)

  if site in ['LCG.GRIF.fr','LCG.M3PEC.fr']:
    if site == 'LCG.GRIF.fr':
      ceList = ['apcce02.in2p3.fr','grid36.lal.in2p3.fr','lpnhe-cream.in2p3.fr','llrcream.in2p3.fr','node74.datagrid.cea.fr']
    if site == 'LCG.M3PEC.fr':
      ceList = ['ce0.bordeaux.inra.fr','ce0.m3pec.u-bordeaux1.fr']

    for ce in ceList:
      j.setDestinationCE(ce)
      name = 'corsikasimtelInstall' + '_' + ce
      j.setName(name)
      Dirac().submit( j )
    DIRAC.exit()

  j.setName('corsikasimtelInstall')
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelInstall( args )
  except Exception:
    Script.gLogger.exception()

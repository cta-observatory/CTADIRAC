#!/usr/bin/env python
"""
  Submit a Corsika Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  runMin:     Min runNumber',
                                     '  runMax:     Max runNumber',
                                     '  cfgFile:    Corsika config file',
                                     '  storageElement: Storage Element',
                                     '  reprocessing configuration (STD)'] ) )

Script.parseCommandLine()

import os

def CorsikaSimtelProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelProdJob import CorsikaSimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelProdJob()
  j.setVersion('prod-2_06052013')

  j.setExecutable('corsika_autoinputs') 

  mode = 'corsika_simtel'
#  mode = 'corsika_standalone'

  ilist = []

  if (len(args) != 5):
    Script.showHelp()
  
  storage_element = args[3]
  if storage_element not in ('CC-IN2P3-Disk','CYF-STORM-Disk','DESY-ZN-Disk'):
    print 'Storage element is not valid'
    Script.showHelp()

  runMin = int(args[0])
  runMax = int(args[1])
  cfgfile = args[2]

  simtelArrayConfig = "STD"
  if args[4] not in ['STD']:
    print "arrayConfig argument %s incorrect"%args[4]
    Script.showHelp()

  simtelArrayConfig = args[4]

  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')
  
  j.setJobGroup('SAC_'+cfgfile[7:])

  j.setInputSandbox( [ cfgfile,'fileCatalog.cfg','prodConfigFile','grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/user/j/johann.cohen-tanugi/PROD2/SVN-PROD2_rev1869.tar.gz'] ) 

  j.setParameters(['fileCatalog.cfg','--template',cfgfile,'--mode',mode,'-D',storage_element,'-S',simtelArrayConfig,'--savecorsika','False'])

  j.setOutputSandbox( ['corsika_autoinputs.log', 'simtel.log'])

  j.setCPUTime(100000)

  #j.setBannedSites(['LCG.CIEMAT.es','LCG.UNI-DORTMUND.de','LCG.M3PEC.fr','LCG.Prague.cz'])

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

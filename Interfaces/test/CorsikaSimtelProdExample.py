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
                                     '  runMax:     Max runNumber' ] ) )
Script.parseCommandLine()

import os

def CorsikaSimtelExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelProdJob import CorsikaSimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelProdJob()
  j.setVersion('prod-2_21122012')

  j.setExecutable('corsika_autoinputs') 

  mode = 'corsika_simtel'
#  mode = 'corsika_standalone'
  storage_element = 'CC-IN2P3-Disk'

  ilist = []

  if (len(args) != 2):
    Script.showHelp()

  runMin = int(args[0])
  runMax = int(args[1])

  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')
  
  j.setInputSandbox( [ 'INPUTS_CTA_PROD2_proton_South','fileCatalog.cfg','prodConfigFile'] )

  j.setParameters(['fileCatalog.cfg','--template','INPUTS_CTA_PROD2_proton_South','--mode',mode,'-D',storage_element])

  j.setOutputSandbox( ['corsika_autoinputs.log', 'simtel.log'])

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

#!/usr/bin/env python
"""
  Submit a Simtel Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Input File List',
                                     '  simtelConfig: cta-ultra5/4MSST',
                                     '  storageElement: Storage Element'] ) )
Script.parseCommandLine()

import os

def SimtelProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.SimtelProdJob import SimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  if (len(args)<1 or len(args)>2):
    Script.showHelp()

  if (len(args)==3):
    storage_element = args[2]
  else :
    storage_element = 'LUPM-Disk'

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    infileLFNList.append(infileLFN)

  j = SimtelProdJob()
 
  j.setVersion('prod-2_08032013')

  j.setParametricInputData(infileLFNList) 

  j.setExecutable('prod2_qgs2') 
  
  simtel_config = args[1]

  j.setParameters(['fileCatalog.cfg','-S',simtel_config,'-D',storage_element])

  j.setInputSandbox( [ 'fileCatalog.cfg','CONFIG'] )

  j.setOutputSandbox( ['simtel.log'])

  name = 'repro_' + simtel_config
  j.setName(name)
 
  j.setJobGroup(name)

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    SimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

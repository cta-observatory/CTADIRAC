#!/usr/bin/env python
"""
  Submit a Corsika Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  storageElement: Storage Element'] ) )
Script.parseCommandLine()

import os

def SimtelProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.SimtelProdJob import SimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac


  LFN_file = 'corsika_lfn.list'

  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    infileLFNList.append(infileLFN)

  
  j = SimtelProdJob()
  j.setVersion('prod-2_21122012')

  j.setExecutable('cta-ultra5') 

  if (len(args)==1):
    storage_element = args[0]
  else :
    storage_element = 'LUPM-Disk'

  j.setParametricInputData(infileLFNList) 

  j.setName('repro%s')

  j.setInputSandbox( [ 'fileCatalog.cfg'] ) 

  j.setParameters(['fileCatalog.cfg','-D',storage_element])
  j.setOutputSandbox( ['simtel.log'])

  j.setCPUTime(100000)

  Script.gLogger.notice( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    SimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

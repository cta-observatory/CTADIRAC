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
                                     ' reprocessing configuration (STD,NSBX3,SCSST,4MSST,ASTRI)'] ) )
Script.parseCommandLine()

import os

def SimtelExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.SimtelJob import SimtelJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  if (len(args) != 2):
    Script.showHelp()

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  if args[1] not in ['STD','4MSST', 'SCSST', 'ASTRI', 'NSBX3', 'NORTH']:
    print "arrayConfig argument %s incorrect"%args[1]
    Script.showHelp()

  simtelArrayConfig = args[1]

  j = SimtelJob()

  j.setVersion('prod-2_22072013')

  j.setParametricInputData(infileLFNList)
#  j.setType( 'CorsikaRepro' )

  name = 'repro_' + simtelArrayConfig
  j.setName(name)

  j.setInputSandbox( [ 'grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/MC/PROD2/SVN-PROD2.tar.gz'] )

  j.setParameters(['-S',simtelArrayConfig])
  j.setOutputSandbox( ['simtel.log'])

#  Retrieve your Output Data  
  sim_out = '*.simtel.gz'
  log_out = '*.log.gz'
  hist_out = '*.hdata.gz'
  j.setOutputData([sim_out,log_out,hist_out])

  j.setCPUTime(100000)
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    SimtelExample( args )
  except Exception:
    Script.gLogger.exception()

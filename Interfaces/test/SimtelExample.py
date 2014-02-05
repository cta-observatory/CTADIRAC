#!/usr/bin/env python
"""
  Submit a Simtel Example Job
"""
from DIRAC.Core.Base import Script
import DIRAC
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Corsika Input File List',
                                     '  reprocessing configuration: STD/NSBX3/4MSST/SCSST/ASTRI/NORTH'] ) )
Script.parseCommandLine()



def SimtelExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.SimtelJob import SimtelJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = SimtelJob()

  j.setVersion('prod-2_15122013')

  if (len(args) != 2):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  if args[1] not in ['STD','4MSST', 'SCSST', 'ASTRI', 'NSBX3', 'NORTH']:
    Script.gLogger.notice("reprocessing configuration is incorrect:",args[1])
    DIRAC.exit( -1 )

  LFN_file = args[0]
  simtelArrayConfig = args[1]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  j.setParametricInputData(infileLFNList)

  name = 'repro_' + simtelArrayConfig
  j.setName(name)

  j.setParameters(['-S',simtelArrayConfig])

  j.setInputSandbox( [ 'grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/MC/PROD2/SVN-PROD2.tar.gz'] )
  j.setOutputSandbox( ['simtel.log'])

#  Retrieve your Output Data  
  sim_out = '*.simtel.gz'
  log_out = '*.log.gz'
  hist_out = '*.hdata.gz'
  j.setOutputData([sim_out,log_out,hist_out])

  j.setCPUTime(100000)

  j.setJobGroup('repro_t1')

  Script.gLogger.info( j._toJDL() )

  res = Dirac().submit( j )
  print res

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    SimtelExample( args )
  except Exception:
    Script.gLogger.exception()

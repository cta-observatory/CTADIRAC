#!/usr/bin/env python
"""
  Submit a EvnDisplay Example Job
"""
from DIRAC.Core.Base import Script
import DIRAC
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Input File List',
				     '  maxFilesPerJob: Max Files Per Job'] ) )
Script.parseCommandLine()

def EvnDispStandloneExample( args = None ) :
  from CTADIRAC.Interfaces.API.EvnDispStandaloneJob import EvnDispStandaloneJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  if len( args ) != 2:
    Script.showHelp()

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  maxFilesPerJob = args[1]
  res = Dirac().splitInputData(infileLFNList,maxFilesPerJob)

  if not res['OK']:
     Script.gLogger.error( 'Failed to splitInputData')
     DIRAC.exit( -1 )

  j = EvnDispStandaloneJob()
  j.setVersion('prod2_140630')

  executable = 'evndisp'
  j.setExecutable( executable )

  j.setGenericParametricInput(res['Value'])

  j.setInputData('%s')

  j.setEvnDispOpt( ['-reconstructionparameter', 'EVNDISP.prod2.reconstruction.runparameter', '-shorttree', '-l2setspecialchannels', 'nofile', '-writenoMCTree'] )

  j.setInputSandbox( ['LFN:/vo.cta.in2p3.fr/user/a/arrabito/evndisp_config.tar.gz'] )
  
  j.setOutputSandbox( ['*.log'])

#  Retrieve your Output Data  
  j.setOutputData( ['*_evndisp.root'] )

  j.setCPUTime(100000)

  j.setName('evndisp')

  Script.gLogger.info( j._toJDL() )
  res = Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    EvnDispStandloneExample( args )
  except Exception:
    Script.gLogger.exception()

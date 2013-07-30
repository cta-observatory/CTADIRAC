#!/usr/bin/env python
"""
  Submit a EvnDisplay Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Input File List',
                                     '  usetrgfile: True/False'] ) )
Script.parseCommandLine()

def EvnDispExample( args = None ) :
  from CTADIRAC.Interfaces.API.EvnDispJob import EvnDispJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = EvnDispJob()
  j.setVersion('prod2_130718')

  executable = 'CTA.convert_hessio_to_VDST'
  j.setExecutable(executable) 

  if not args:
    Script.showHelp()

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  j.setParametricInputData(infileLFNList)

  usetrgfile = 'False'
  if len(args) == 2:
    usetrgfile = args[1]

  j.setUseTrgFile(usetrgfile)

  j.setConverterOpt(['-f','1','-c','Aar.peds.root','-a','EVNDISP.CTA.runparameter/DetectorGeometry/CTA.prod2.2a.lis'])

  j.setEvnDispOpt(['-reconstructionparameter','EVNDISP.prod2.reconstruction.runparameter'])
  
  j.setInputSandbox( [ 'LFN:/vo.cta.in2p3.fr/user/a/arrabito/EvnDisp/Aar.peds.root' ])

  j.setOutputSandbox( ['*.log'])

#  Retrieve your Output Data  
  j.setOutputData('*_evndisp.root')

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )
  
if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    EvnDispExample( args )
  except Exception:
    Script.gLogger.exception()

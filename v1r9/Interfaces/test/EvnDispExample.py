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
                                     '  layoutlist: Layout File List',
                                     '  usetrgfile: True/False'] ) )
Script.parseCommandLine()

def EvnDispExample( args = None ) :
  from CTADIRAC.Interfaces.API.EvnDispJob import EvnDispJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = EvnDispJob()
  j.setVersion('prod2_130718')

  executable = 'CTA.convert_hessio_to_VDST'
  j.setExecutable(executable) 

  #if not args:
  if len(args) < 2:
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
  if len(args) == 3:
    usetrgfile = args[2]

  j.setUseTrgFile(usetrgfile)
 
  layoutlist = args[1]
  j.setLayoutList(layoutlist)

  j.setConverterOpt(['-f','1','-c','Aar.peds.root'])
  
  j.setEvnDispOpt(['-reconstructionparameter','EVNDISP.prod2.reconstruction.runparameter','-shorttree','-l2setspecialchannels','nofile','-writenoMCTree'])
  
  j.setInputSandbox( [ 'LFN:/vo.cta.in2p3.fr/user/a/arrabito/EvnDisp/Aar.peds.root',layoutlist])

  j.setOutputSandbox( ['*.log'])

#  Retrieve your Output Data  
  j.setOutputData(['*_evndisp.root']) 

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )
  
if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    EvnDispExample( args )
  except Exception:
    Script.gLogger.exception()

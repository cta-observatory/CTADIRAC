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
				     '  maxFilesPerJob: Max Files Per Job',
                                     '  layoutlist: Layout File List',
                                     '  usetrgfile: True/False (optional, default is False)'] ) )
Script.parseCommandLine()

def EvnDispExample( args = None ) :
  from CTADIRAC.Interfaces.API.EvnDispJob import EvnDispJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  if len(args)!=3 and len(args)!=4:
    Script.showHelp()

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  usetrgfile = 'False'
  if len(args) == 4:
    usetrgfile = args[3]
 
  maxFilesPerJob = args[1]

  if (maxFilesPerJob > 1 and usetrgfile == 'True'):
    Script.gLogger.notice("Multiple input files per job are not compatible with usetrgfile=True")
    Script.gLogger.notice("Set maxFilesPerJob=1")
    DIRAC.exit( -1 )

  layoutlist = args[2]

  res = Dirac().splitInputData(infileLFNList,maxFilesPerJob)

  if not res['OK']:
     Script.gLogger.error( 'Failed to splitInputData')
     DIRAC.exit( -1 )

 #########################################################################

  j = EvnDispJob()
  j.setVersion('prod2_131218')

  executable = 'CTA.convert_hessio_to_VDST'
  j.setExecutable(executable)

  j.setGenericParametricInput(res['Value'])

  j.setInputData('%s')

  j.setUseTrgFile(usetrgfile)
  j.setLayoutList(layoutlist)

  j.setConverterOpt(['-f','1','-c','Calibration/Aar.peds.root'])
  
  j.setEvnDispOpt(['-reconstructionparameter','EVNDISP.prod2.reconstruction.runparameter','-shorttree','-l2setspecialchannels','nofile','-writenoMCTree'])
  
  j.setInputSandbox( [layoutlist, 'LFN:/vo.cta.in2p3.fr/user/a/arrabito/evndisp_config.tar.gz'])

  j.setOutputSandbox( ['*.log'])

#  Retrieve your Output Data  
  j.setOutputData(['*_evndisp.root'])

  j.setCPUTime(100000)

  j.setName('evndisp')

  Script.gLogger.info( j._toJDL() )
  res = Dirac().submit( j )
  #print res

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    EvnDispExample( args )
  except Exception:
    Script.gLogger.exception()

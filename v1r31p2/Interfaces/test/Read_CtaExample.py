#!/usr/bin/env python
"""
  Submit a read_cta Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Input File List'] ) )
Script.parseCommandLine()

def Read_CtaExample( args = None ) :
  from CTADIRAC.Interfaces.API.Read_CtaJob import Read_CtaJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = Read_CtaJob()
  j.setVersion('prod-2_15122013')

  if len(args)!=1:
    Script.showHelp()

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  j.setParametricInputData(infileLFNList)
  
  j.setRead_CtaOpt(['-r', '4', '-u', '--integration-scheme', '4', '--integration-window', '7,3', '--tail-cuts', '6,8', '--min-pix', '2', '--min-amp', '20', '--type', '1,0,0,400', '--tail-cuts', '9,12', '--min-amp', '20', '--type', '2,0,0,100', '--tail-cuts', '8,11', '--min-amp', '19', '--type', '3,0,0,40', '--tail-cuts', '6,9', '--min-amp', '15', '--type', '4,0,0,15', '--tail-cuts', '3.7,5.5', '--min-amp', '8', '--dst-level', '0', '--powerlaw', '-2.57'])

#  Retrieve your Output Data  
  j.setOutputData(['*simtel-dst.gz','*hdata-dst.gz'])

  j.setName('read_cta')
  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  res = Dirac().submit( j )
  print res

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    Read_CtaExample( args )
  except Exception:
    Script.gLogger.exception()

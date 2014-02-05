#!/usr/bin/env python
"""
  Submit a Corsika Example Job
"""
import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  runMin:     Min runNumber',
                                     '  runMax:     Max runNumber',
                                     '  cfgFile:    Corsika config file'] ) )

Script.parseCommandLine()


def CorsikaExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelJob import CorsikaSimtelJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelJob()

  j.setVersion('prod-2_15122013')

  j.setExecutable('corsika_autoinputs')

  if (len(args) != 3):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  runMin = int(args[0])
  runMax = int(args[1])
  cfgfile = args[2]

  ilist = []
  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')

  j.setInputSandbox( [cfgfile] )

  j.setParameters(['--template',cfgfile,'--mode','corsika_standalone'])

  j.setOutputSandbox( ['corsika_autoinputs.log'])

#  Retrieve your Output Data  
  j.setOutputData(['*.corsika.gz','*.corsika.tar.gz'])

  j.setCPUTime(200000)

  Script.gLogger.info( j._toJDL() )

  res = Dirac().submit( j )
  print res

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaExample( args )
  except Exception:
    Script.gLogger.exception()

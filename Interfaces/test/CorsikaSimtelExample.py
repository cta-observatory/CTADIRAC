#!/usr/bin/env python
"""
  Submit a CorsikaSimtel Example Job
"""
import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  runMin:     Min runNumber',
                                     '  runMax:     Max runNumber',
                                     '  cfgFile:    Corsika config file',
                                     '  reprocessing configuration: STD/NSBX3/4MSST/SCSST/ASTRI/NORTH'] ) )

Script.parseCommandLine()


def CorsikaSimtelExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelJob import CorsikaSimtelJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelJob()

  j.setVersion('prod-2_15122013')

  j.setExecutable('corsika_autoinputs')

  if len(args) != 4:
    Script.showHelp()

  if args[3] not in ['STD', '4MSST', 'SCSST', 'ASTRI', 'NSBX3', 'NORTH', 'SCMST']:
    Script.gLogger.notice("reprocessing configuration is incorrect:",args[3])
    DIRAC.exit( -1 )
  
  runMin = int(args[0])
  runMax = int(args[1])
  cfgfile = args[2]
  simtelArrayConfig = args[3]

  ilist = []
  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')

  j.setInputSandbox( [cfgfile,'grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/MC/PROD2/SVN-PROD2.tar.gz'] )

  j.setParameters(['--template',cfgfile,'--mode','corsika_simtel','-S',simtelArrayConfig])

  j.setOutputSandbox( ['corsika_autoinputs.log', 'simtel.log','applicationLog.txt'])

#  Retrieve your Output Data  
  corsika_out = '*.corsika.gz'
  corsikatar_out = '*.corsika.tar.gz'
  sim_out = '*.simtel.gz'
  log_out = '*.log.gz'
  hist_out = '*.hdata.gz'
  j.setOutputData([corsika_out,corsikatar_out,sim_out,log_out,hist_out])

  j.setCPUTime(200000)

  Script.gLogger.info( j._toJDL() )

  res = Dirac().submit( j )
  print res

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelExample( args )
  except Exception:
    Script.gLogger.exception()

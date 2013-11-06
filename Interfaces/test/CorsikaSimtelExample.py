#!/usr/bin/env python
"""
  Submit a CorsikaSimtel Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  runMin:     Min runNumber',
                                     '  runMax:     Max runNumber',
                                     '  cfgFile:    Corsika config file',
                                     '  reprocessing configuration: 4MSST/SCSST/ASTRI/NSBX3/STD/NORTH'] ) )

Script.parseCommandLine()

import os

def CorsikaSimtelExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelJob import CorsikaSimtelJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelJob()
  j.setVersion('prod-2_22072013')

  j.setExecutable('corsika_autoinputs')

  mode = 'corsika_simtel'
#  mode = 'corsika_standalone'

  ilist = []

  if (len(args) != 4):
    Script.showHelp()

  runMin = int(args[0])
  runMax = int(args[1])
  cfgfile = args[2]

  simtelArrayConfig = "STD"
  if args[3] not in ['STD','4MSST', 'SCSST', 'ASTRI', 'NSBX3', 'NORTH']:
    print "arrayConfig argument %s incorrect"%args[4]
    Script.showHelp()

  simtelArrayConfig = args[3]

  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')

  j.setInputSandbox( [ cfgfile,'grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/MC/PROD2/SVN-PROD2.tar.gz'] )

  j.setParameters(['--template',cfgfile,'--mode',mode,'-S',simtelArrayConfig])
 
  j.setOutputSandbox( ['corsika_autoinputs.log', 'simtel.log'])

#  Retrieve your Output Data  
  corsika_out = '*.corsika.gz'
  corsikatar_out = '*.corsika.tar.gz'
  sim_out = '*.simtel.gz'
  log_out = '*.log.gz'
  hist_out = '*.hdata.gz'
  j.setOutputData([corsika_out,corsikatar_out,sim_out,log_out,hist_out])

  j.setCPUTime(100000)

  j.setBannedSites(['LCG.UNI-DORTMUND.de','LCG.PIC.es'])

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelExample( args )
  except Exception:
    Script.gLogger.exception()

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
                                     '  reprocessing configuration: 4MSST/SCSST/ASTRI/NSBX3/STD/SCMST/NORTH/3INROW'] ) )

Script.parseCommandLine()

import os

def CorsikaSimtelProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelProdJob import CorsikaSimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelProdJob()
  j.setVersion('prod-2_15122013')

  j.setExecutable('corsika_autoinputs') 

#  mode = 'corsika_simtel'
#  mode = 'corsika_standalone'
  mode = 'corsika_simtel_dst'

  ilist = []

  if (len(args) != 4):
    Script.showHelp()

  runMin = int(args[0])
  runMax = int(args[1])
  cfgfile = args[2]

  simtelArrayConfig = "STD"
  if args[3] not in ['STD','6INROW','NORTH','3INROW','SCMST']:
    print "arrayConfig argument %s incorrect"%args[4]
    Script.showHelp()

  simtelArrayConfig = args[3]

  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')
  
  j.setJobGroup(cfgfile[11:])

  j.setInputSandbox( [ cfgfile,'fileCatalog.cfg','prodConfigFile','grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/user/a/arrabito/PROD2/SVN-PROD2_rev2350.tar.gz'] ) 

  j.setParameters(['fileCatalog.cfg','--template',cfgfile,'--mode',mode,'-S',simtelArrayConfig,'--savecorsika','False'])
 
  j.setOutputSandbox( ['corsika_autoinputs.log', 'simtel.log','applicationLog.txt'])

  j.setCPUTime(100000)

  j.setBannedSites(['LCG.UNI-DORTMUND.de','LCG.PIC.es'])

  Script.gLogger.info( j._toJDL() )
  res = Dirac().submit( j )

  print res


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

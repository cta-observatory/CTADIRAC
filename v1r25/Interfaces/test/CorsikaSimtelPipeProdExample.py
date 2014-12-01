#!/usr/bin/env python
"""
  Submit a CorsikaSimtelPipe Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  runMin:     Min runNumber',
                                     '  runMax:     Max runNumber',
                                     '  cfgFile:    Corsika config file',
                                     '  reprocessing configuration: 3INROW'] ) )

Script.parseCommandLine()

import os

def CorsikaSimtelPipeProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.CorsikaSimtelPipeProdJob import CorsikaSimtelPipeProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac
  
  j = CorsikaSimtelPipeProdJob()
  j.setVersion('prod-2_13112014')

  j.setExecutable('corsika_autoinputs') 

  mode = 'corsika_simtel_dst'

  ilist = []

  if (len(args) != 4):
    Script.showHelp()

  runMin = int(args[0])
  runMax = int(args[1])
  cfgfile = args[2]

  simtelArrayConfig = '3INROW'
  if args[3] not in ['3INROW']:
    print "arrayConfig argument %s incorrect"%args[4]
    Script.showHelp()

  simtelArrayConfig = args[3]

  for i in range(runMin,runMax+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)                                                                                         
  j.setName('run%s')
  
  j.setJobGroup(cfgfile[11:])

  j.setInputSandbox( [ cfgfile,'fileCatalog.cfg','prodConfigFile'] ) 

  j.setParameters(['fileCatalog.cfg','--template',cfgfile,'--mode',mode,'-S',simtelArrayConfig,'--savecorsika','False'])
 
  j.setOutputSandbox( ['corsika_autoinputs.log'])
  
  j.setCPUTime(720000)

  j.setBannedSites(['LCG.UNI-DORTMUND.de'])

  Script.gLogger.info( j._toJDL() )
  res = Dirac().submit( j )



if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelPipeProdExample( args )
  except Exception:
    Script.gLogger.exception()

#!/usr/bin/env python
"""
  Submit an Example Hap Converter Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [Site] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Requested Site' ] ) )
Script.parseCommandLine()

import os

def CorsikaSimtelProdExample( destination = None ) :
  from CTADIRAC.Interfaces.API.CorsikaSimtelProdJob import CorsikaSimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  
  j = CorsikaSimtelProdJob()
  j.setVersion('clean_23012012')

  executable = 'corsika_autoinputs'
  j.setExecutable(executable) 

 

  ilist = []
  for i in range(1,5):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)
#######################################                                                                                           
  j.setName('run%s')

  j.setParameters(['fileCatalog.cfg','--run','corsika','--template','INPUTS_CTA_ULTRA3_proton'])

  if destination:
    j.setDestination( destination )

  j.setInputSandbox( [ 'INPUTS_CTA_ULTRA3_proton', 'cta-corsikasimtelprod.py','fileCatalog.cfg','prodConfigFile'] )
  outlog = executable + '.log'
  j.setOutputSandbox( [outlog])

  sim_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Data/*.simtel.gz'
  log_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Log/*.log.gz'
  hist_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Histograms/*.hdata.gz'
  corsika_out = 'run%s.corsika.gz'
  corsikatar_out = 'corsika_run%s.tar.gz'


  j.setCPUTime(100000)
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()
  print "args:",args
  CorsikaSimtelProdExample( args )



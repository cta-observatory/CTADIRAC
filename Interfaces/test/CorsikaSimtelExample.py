#!/usr/bin/env python
"""
  Submit a Corsika Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [NJobs] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  NJobs:     Number of Jobs' ] ) )
Script.parseCommandLine()

def CorsikaSimtelExample( args = None ) :
  from CTADIRAC.Interfaces.API.CorsikaSimtelJob import CorsikaSimtelJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = CorsikaSimtelJob()
  j.setVersion('clean_23012012')

  executable = 'corsika_autoinputs'
  j.setExecutable(executable) 

  ilist = []
  if not args:
    Script.showHelp()

  NJobs = int(args[0])

  for i in range(1,NJobs+1):
    run_number = '%06d' % i
    ilist.append(run_number)

  j.setGenericParametricInput(ilist)
  j.setName('run%s')
  j.setInputSandbox( [ 'INPUTS_CTA_ULTRA3_proton'] )
  
  j.setParameters(['--run','corsika','--template','INPUTS_CTA_ULTRA3_proton'])

  outlog = executable + '.log'

  j.setOutputSandbox( [outlog])

  corsikatar_out = 'corsika_run%s.tar.gz'

  j.setOutputData(['run%s/cta-ultra3-test.corsika.gz',corsikatar_out])

# To retrieve Output Data from simtel_array 
#  sim_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Data/*.simtel.gz'
#  log_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Log/*.log.gz'
#  hist_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Histograms/*.hdata.gz'
#  j.setOutputData([sim_out,log_out,hist_out,corsikatar_out])

  j.setCPUTime(100000)

  if destination:
    j.setDestination( destination )

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelExample( args )
  except Exception:
    Script.gLogger.exception()


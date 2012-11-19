#!/usr/bin/env python
"""
  Submit a CorsikaSimtel Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [NJobs] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  NJobs:     Number of Jobs' ] ) )
Script.parseCommandLine()

def CorsikaSimtelUserExample( args = None ) :
  from CTADIRAC.Interfaces.API.CorsikaSimtelUserJob import CorsikaSimtelUserJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  j = CorsikaSimtelUserJob()
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

### Your sim_tel directory with your own configurations ######
  myconfigdir = 'mysim_telarray'

  j.setInputSandbox( [ 'INPUTS_CTA_ULTRA3_proton',myconfigdir] )
  
  j.setParameters(['--run','corsika','--template','INPUTS_CTA_ULTRA3_proton','--simconfig',myconfigdir,'--simexe','sim_telarray','--help'])

  j.setOutputSandbox( ['corsika_autoinputs.log','simtel.log.gz'])

#  Retrieve your Output Data  
  corsika_out = 'corsika_run%s.corsika.gz'
  corsikatar_out = 'corsika_run%s.tar.gz'
  j.setOutputData([corsika_out,corsikatar_out])

 # sim_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Data/*.simtel.gz'
 # log_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Log/*.log.gz'
 # hist_out = 'Data/sim_telarray/cta-ultra3/0.0deg/Histograms/*.hdata.gz'
 # j.setOutputData([corsika_out,corsikatar_out,sim_out,log_out,hist_out])


  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    CorsikaSimtelUserExample( args )
  except Exception:
    Script.gLogger.exception()


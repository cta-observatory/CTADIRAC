#!/usr/bin/env python
"""
  Submit a simtel Job
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [version]' % Script.scriptName,
                                     'Arguments:',
                                     '  version: prod-2_21122012/prod-2_08032013/prod-2_06052013'] ) )

Script.parseCommandLine()

import os


def simteljob(args = None ):

  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.Job import Job

  if (len(args)!=1):
    Script.showHelp()

  version = args[0]

  user_script = './run_simtel.sh'
  
  infileLFNList = ['/vo.cta.in2p3.fr/MC/PROD2/Config_120213/prod-2_21122012_corsika/proton/Data/044xxx/proton_20.0_180.0_alt2662.0_run044019.corsika.gz',
'/vo.cta.in2p3.fr/MC/PROD2/Config_120213/prod-2_21122012_corsika/proton/Data/044xxx/proton_20.0_180.0_alt2662.0_run044085.corsika.gz']


  for infileLFN in infileLFNList:
    filein = os.path.basename(infileLFN)

    j = Job()

    j.setInputSandbox( ['cta-simtel.py', user_script] )  
    j.setInputData(infileLFN)
  
    user_args = []
    user_args = [filein]
  
    executablestr = "%s %s %s" % ( version, user_script, ' '.join( user_args ) )

    j.setExecutable('./cta-simtel.py', executablestr)

    sim_out = 'Data/sim_telarray/cta-ultra5/0.0deg/Data/*.simtel.gz'
    log_out = 'Data/sim_telarray/cta-ultra5/0.0deg/Log/*.log.gz'
    hist_out = 'Data/sim_telarray/cta-ultra5/0.0deg/Histograms/*.hdata.gz'
   
    j.setOutputData([sim_out,log_out,hist_out])
    j.setOutputSandbox('simtel.log')
    j.setName(user_script)
    j.setCPUTime(100000)

    Script.gLogger.info( j._toJDL() )

    Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    simteljob( args )
  except Exception:
    Script.gLogger.exception()




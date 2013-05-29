#!/usr/bin/env python

from DIRAC.Core.Base import Script
import os

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [version]' % Script.scriptName,
                                     'Arguments:',
                                     '  version: prod-2_21122012/prod-2_08032013/prod-2_06052013'] ) )

Script.parseCommandLine()

def read_hessjob():

  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Interfaces.API.Job import Job

  if (len(args)!=1):
    Script.showHelp()

  version = args[0]

  user_script = './read_hess2dst.sh'

  sim_file = 'simtel_file.list'

  infileLFNList = ['/vo.cta.in2p3.fr/MC/PROD2/Config_310113/prod-2_21122012_corsika/gamma/prod-2_06052013_simtel_STD/Data/002xxx/gamma_20.0_180.0_alt2662.0_run002997.simtel.gz', 
'/vo.cta.in2p3.fr/MC/PROD2/Config_310113/prod-2_21122012_corsika/gamma/prod-2_06052013_simtel_STD/Data/002xxx/gamma_20.0_180.0_alt2662.0_run002998.simtel.gz']

  f = open(sim_file,'w')

  for infileLFN in infileLFNList:
    filein = os.path.basename(infileLFN)
    f.write(filein)
    f.write('\n')

  f.close()
   
  j = Job()

  j.setInputData(infileLFNList)
  
  options = []
  options = [sim_file]
  
  executablestr = "%s %s %s" % ( version, user_script, ' '.join( options ) )

  j.setExecutable('./cta-read_hess.py', executablestr)

  j.setInputSandbox( ['cta-read_hess.py', user_script, sim_file] )

  j.setOutputSandbox( ['read_hess.log'] )

  j.setOutputData(['*dst.gz'])

  j.setName(user_script)

  j.setCPUTime(100000)
 
  Script.gLogger.info( j._toJDL() )

  Dirac().submit( j )

if __name__ == '__main__':

  try:
    read_hessjob()
  except Exception:
    Script.gLogger.exception()


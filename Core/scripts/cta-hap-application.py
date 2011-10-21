#!/usr/bin/env python
import DIRAC

def main():

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  from CTADIRAC.Core.Workflow.Modules.HapApplication import HapApplication

  DIRAC.gLogger.notice( 'Executing a Hap Application' )

  args = Script.getPositionalArgs()
  DIRAC.gLogger.notice( 'Arguments:', args )

  ha = HapApplication()
  ha.setSoftwarePackage('HAP/v0.1/HAP')
  # There is a bug in the Job.py class that produce a duplicated is the first argument
  if args[1].find( args[0] ) == 0:
#    rm.rootMacro = args[1]
    ha.rootArguments = args[1:]
  else:
#    rm.rootMacro = args[0]
    ha.rootArguments = args[0:]

  res = ha.execute()

  if not res['OK']:
    DIRAC.exit( -1 )

  DIRAC.exit()

if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

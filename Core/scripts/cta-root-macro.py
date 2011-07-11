#!/usr/bin/env python 
import DIRAC

def main():

  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  from CTADIRAC.Core.Workflow.Modules.RootMacro import RootMacro

  DIRAC.gLogger.notice( 'Executing a Root Macro' )

  args = Script.getPositionalArgs()
  DIRAC.gLogger.notice( 'Arguments:', args )

  rm = RootMacro()
  rm.setSoftwarePackage()
  # There is a bug in the Job.py class that produce a duplicated is the first argument
  if args[1].find( args[0] ) == 0:
    rm.rootMacro = args[1]
    rm.rootArguments = args[2:]
  else:
    rm.rootMacro = args[0]
    rm.rootArguments = args[1:]

  res = rm.execute()

  if not res['OK']:
    DIRAC.exit( -1 )

  DIRAC.exit()

if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

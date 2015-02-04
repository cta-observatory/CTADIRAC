#!/usr/bin/env python

import DIRAC
import os

def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

def main():

  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea

  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  package = args[0]
  version = args[1]

  DIRAC.gLogger.notice('Software area:', sharedArea())

  path = sharedArea()

  DIRAC.gLogger.notice('Directory content:', path)
  cmd = 'ls -l ' + path
  if(os.system(cmd)):
    DIRAC.exit( -1 )

  path = os.path.join(path,package)
  DIRAC.gLogger.notice('Directory content:', path)
  cmd = 'ls -l ' + path
  if(os.system(cmd)):
    DIRAC.exit( -1 )

  path = os.path.join(path,package,version)
  DIRAC.gLogger.notice('Directory content:', path)
  cmd = 'ls -l ' + path
  if(os.system(cmd)):
    DIRAC.exit( -1 )

  DIRAC.exit()


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

#!/usr/bin/env python

import DIRAC
import os

def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

def main():

  from DIRAC import gLogger
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  package = args[0]
  version = args[1]

  path = os.path.join('${VO_VO_CTA_IN2P3_FR_SW_DIR}/software',package,version)
  cmd = 'rm -Rf ' + path

  if(os.system(cmd)):
    DIRAC.exit( -1 )

  DIRAC.gLogger.notice('Directory content after cleaning')
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

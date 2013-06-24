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
  version = args[0]

  path = os.path.join('${VO_VO_CTA_IN2P3_FR_SW_DIR}/software/corsika_simhessarray',version)
  cmd = 'rm -R ' + path

  if(os.system(cmd)):
    DIRAC.exit( -1 )

  DIRAC.exit()


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

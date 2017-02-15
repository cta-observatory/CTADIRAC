#!/usr/bin/env python

import os

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Subprocess import systemCall

def sendOutput( stdid, line ):
  f = open( 'Corsika_Log.txt', 'a' )
  f.write( line )
  f.write( '\n' )
  f.close()

def main():
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  from DIRAC.Interfaces.API.Dirac import Dirac

  # get arguments
  input_card = args[0]
  outprefix = ""
  if len(args) ==2:
    outprefix = args[1]

  #### get Parameter and set run_number #########
  if os.environ.has_key( 'JOBID' ):
    jobID = os.environ['JOBID']
    dirac = Dirac()
    res = dirac.getJobJDL( jobID )
    run_number = res['Value']['Parameter.run']

  # # run corsika
  cmdTuple = ['./dirac_prod3_corsika_only', '--run', run_number, input_card]
  DIRAC.gLogger.notice( 'Executing command tuple:', cmdTuple )
  res = systemCall( 0, cmdTuple, sendOutput )
  if not res['OK']:
    return res

  # ## rename output file
  #outfile = 'run%s.corsika.gz' % run_number
  outfile = '%srun%s.corsika.gz' % (outprefix, run_number)
  cmd = 'mv Data/corsika/*/*corsika.gz %s' % outfile
  if( os.system( cmd ) ):
    DIRAC.exit( -1 )

  return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
  try:
    res = main()
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

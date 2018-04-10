""" Prod3 MC Script to run corsika with a given input card
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s runMin runMax input_card' % Script.scriptName,
                                     'Arguments:',
                                     '  runMin: minimum run number',
                                     '  runMax: maximum run number',
                                     '  input_card: corsika input card',
                                     '\ne.g: %s 1 10 INPUTS_CTA-trg-test-prod3-SST1_proton' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from CTADIRAC.Interfaces.API.Prod3MCUserJob import Prod3MCUserJob
from DIRAC.Interfaces.API.Dirac import Dirac

def runProd3( args = None ):
  """ Simple wrapper to create a Prod3MCUserJob and setup parameters
      from positional arguments given on the command line.
      
      Parameters:
      args -- a list of 3 strings corresponding to job arguments
              runMin runMax input_card
  """
  # get arguments
  runMin = int( args[0] )
  runMax = int( args[1] )
  input_card = args[2]
  
  # ## Create Prod3 User Job
  job = Prod3MCUserJob()

  # set package version and corsika input card. to be set before setupWorkflow
  job.setPackage('corsika_simhessarray')
  job.setVersion( '2017-04-19' )
  job.setInputCard( input_card )

  # ## setup workflow: set executable and parameters
  job.setupWorkflow()

  # # set run_number as parameter for parametric jobs
  ilist = []
  for run_number in range( runMin, runMax + 1 ):
    ilist.append( str( run_number ) )
  job.setParameterSequence( 'run', ilist )

  # ## set job attributes
  job.setName( 'corsika' )
  job.setInputSandbox( [input_card,'dirac_prod3_corsika_only'] )
  job.setOutputSandbox( ['*Log.txt'] )
  job.setOutputData( ['*corsika.gz'] )

  # # submit job
  dirac = Dirac()
  res = dirac.submit( job )
  # debug
  Script.gLogger.info( 'Submission Result: ', res )
  Script.gLogger.info( job.workflow )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 3 ):
    Script.showHelp()
  try:
    res = runProd3( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( res['Value'] )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

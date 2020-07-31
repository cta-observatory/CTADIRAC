""" Prod3 MC Script to run read_cta for users
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s simtel input file list' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfiles: simtel input file LFNs list',
                                     '\ne.g: %s simtel.list' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from CTADIRAC.Interfaces.API.Prod3MCUserJob import Prod3MCUserJob
from DIRAC.Interfaces.API.Dirac import Dirac

def submitWMS( job, infileList ):
  """ Submit the job locally or to the WMS  """

  dirac = Dirac()
  job.setParameterSequence( 'InputData', infileList, addToWorkflow = 'ParametricInputData' )
  job.setOutputData( ['*simtel-dst0.gz'], outputPath='read_cta_data' )
  job.setName( 'readctajob' )
  # To allow jobs run at other sites than the site where the InputData are located
  #job.setType( 'DataReprocessing' )

  res = dirac.submitJob( job )

  if res['OK']:
    Script.gLogger.info( 'Submission Result: ', res['Value'] )

  return res

def runProd3( args = None ):
  """ Simple wrapper to create a Prod3MCUserJob and setup parameters
      from positional arguments given on the command line.
      
      Parameters:
      args -- simtel input file list
  """

  # get arguments
  infile = args[0]
  f = open( infile, 'r' )

  infileList = []
  for line in f:
    infile = line.strip()
    if line != "\n":
      infileList.append( infile )

  ### Main script
  job = Prod3MCUserJob()

  # set package version: to be set before setupWorkflow
  job.setPackage('corsika_simhessarray')
  job.setVersion( '2017-09-01' )
  job.runType = 'readcta'
  
   ## set job attributes
  job.setOutputSandbox( ['*Log.txt'] )
 
  # ## setup workflow: set executable and parameters
  job.setupWorkflow()

  ## group input files and submit
  res = submitWMS( job, infileList )

  # debug
    #Script.gLogger.info( job.workflow )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 1 ):
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

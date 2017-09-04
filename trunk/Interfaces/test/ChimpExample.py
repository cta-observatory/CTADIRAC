""" Chimp Script to create a Transformation
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s mode infile' % Script.scriptName,
                                     'Arguments:',
                                     '  infile: ascii file with input files LFNs',
                                     '\ne.g: %s Paranal_gamma_North.list TS' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from CTADIRAC.Interfaces.API.ChimpJob import ChimpJob
from DIRAC.Interfaces.API.Dirac import Dirac

def submitTS( job, infileList ):
  """ Create a transformation executing the job workflow  """
  t = Transformation()
  tc = TransformationClient()
  t.setType( "DataReprocessing" )
  t.setDescription( "Chimp example" )
  t.setLongDescription( "Chimp analysis" )  # mandatory
  t.setGroupSize(1)
  t.setBody ( job.workflow.toXML() )

  res = t.addTransformation()  # Transformation is created here

  if not res['OK']:
    print res['Message']
    DIRAC.exit( -1 )

  t.setStatus( "Active" )
  t.setAgentType( "Automatic" )
  transID = t.getTransformationID()
  tc.addFilesToTransformation( transID['Value'], infileList )  # Files added here

  return res

#########################################################

def runChimp( args = None ):
  """ Simple wrapper to create ChimpJob and setup parameters
      from positional arguments given on the command line.
      
      Parameters:
      args -- infile mode
  """
  # get arguments
  infile = args[0]
  f = open( infile, 'r' )

  infileList = []
  for line in f:
    infile = line.strip()
    if line != "\n":
      infileList.append( infile )

##################################
  job = ChimpJob( cpuTime = 432000 )  # to be adjusted!!

  ### Main Script ###
  job.setName( 'ChimpTest' )
  
  # package and version
  job.setPackage( 'chimp' )
  job.setVersion( '2015-10-06' )

  # set Meta data
  job.setMD( infileList[0] )

  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow()

  res = submitTS( job, infileList )
    
  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 1 ):
    Script.showHelp()
  try:
    res = runChimp( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

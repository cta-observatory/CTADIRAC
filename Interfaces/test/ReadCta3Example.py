""" Script to create a Transformation running read_cta
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
from CTADIRAC.Interfaces.API.ReadCta3Job import ReadCta3Job

def submitTS( job, infileList ):
  """ Create a transformation executing the job workflow  """
  t = Transformation()
  tc = TransformationClient()
  t.setType( "DataReprocessing" )
  t.setDescription( "ReadCta3 example" )
  t.setLongDescription( "ReadCta analysis" )  # mandatory
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

def runReadCta3( args = None ):
  """ Simple wrapper to create a ReadCta3Job and setup parameters
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
  job = ReadCta3Job(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  job.setName( 'ReadCta3Test' )

  # package and version
  job.setPackage( 'corsika_simhessarray' )
  job.setVersion( '2015-10-20' )

  # set ReadCta Meta data
  job.setReadCtaMD( infileList[0] )

  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow()

  # submit to the Transformation System
  res = submitTS( job, infileList )

  # debug
    #Script.gLogger.info( job.workflow )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) < 1):
    Script.showHelp()
  try:
    res = runReadCta3( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )
""" Script to create a Transformation running merge_simtel for 3HB8 merging
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
#from CTADIRAC.Interfaces.API.Prod3Merge3HB8Job import Prod3Merge3HB8Job
from Prod3Merge3HB8Job import Prod3Merge3HB8Job
from DIRAC.Interfaces.API.Dirac import Dirac


# Submit to TS
def submitTS( job, infileList ):
  """ Create a transformation executing the job workflow  """
  t = Transformation()
  tc = TransformationClient()
  t.setType( "SimtelMerging" )
  t.setDescription( "Runs merge_simtel for array 3HB8" )
  t.setLongDescription( "Merging array 3HB8 analysis" )  # mandatory
  t.setGroupSize(10)
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
  
# Submit to WMS for testing  

def submitWMS( job, infileList ):
  """ Submit the job locally or to the WMS  """
  #job.setDestination( 'LCG.IN2P3-CC.fr' )
  job.setInputData(infileList[:2])
  job.setInputSandbox( ['cta-prod3-get-matching-data.py'] )   

  dirac = Dirac()
  #res = dirac.submit( job, "local" )
  res = dirac.submit( job )

  Script.gLogger.notice( 'Submission Result: ', res )
  return res

#########################################################

def runMergeSimtel( args = None ):
  """ Simple wrapper to create a Prod3Merge3HB8Job and setup parameters
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
  job = Prod3Merge3HB8Job(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  job.setName( 'Prod3Merge3HB8JobTest' )

  # package and version
  job.setPackage( 'corsika_simhessarray' )
  job.setVersion( '2015-10-20-p2' )

  # set ReadCta Meta data
  job.setReadCtaMD( infileList[0] )

  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow()

  # submit to the Transformation System
  res = submitTS( job, infileList )
  #res = submitWMS(job, infileList)
  # debug
    #Script.gLogger.info( job.workflow )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) < 1):
    Script.showHelp()
  try:
    res = runMergeSimtel( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

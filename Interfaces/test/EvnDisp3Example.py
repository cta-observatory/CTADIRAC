""" EvnDisp Script to create a Transformation
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s mode infile' % Script.scriptName,
                                     'Arguments:',
                                     '  infile: ascii file with input files LFNs',
                                     '  mode: WMS, TS (default)',
                                     '\ne.g: %s Paranal_gamma_North.list TS' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from CTADIRAC.Interfaces.API.EvnDisp3Job import EvnDisp3Job
from DIRAC.Interfaces.API.Dirac import Dirac

def submitTS( job, infileList ):
  """ Create a transformation executing the job workflow  """
  t = Transformation()
  tc = TransformationClient()
  t.setType( "DataReprocessing" )
  t.setDescription( "EvnDisp3 example" )
  t.setLongDescription( "EvnDisplay analysis" )  # mandatory
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

def submitWMS( job, infileList, nbFileperJob ):
  """ Submit the job locally or to the WMS  """

  dirac = Dirac()
  res = Dirac().splitInputData( infileList, nbFileperJob )
  if not res['OK']:
    Script.gLogger.error( 'Failed to splitInputData' )
    DIRAC.exit( -1 )

  job.setGenericParametricInput( res['Value'] )
  job.setInputData( '%s' )

  job.setJobGroup( 'EvnDisp-proton-partx09' )

  res = dirac.submit( job )

  Script.gLogger.info( 'Submission Result: ', res['Value'] )
  return res

#########################################################

def runEvnDisp3( args = None ):
  """ Simple wrapper to create a EnDisp3Job and setup parameters
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

  if len( args ) == 1:
    mode = 'TS'
  else:
    mode = args[1]

##################################
  job = EvnDisp3Job(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  # override for testing
  job.setName( 'EvnDisp3Test' )
  
  # package and version
  job.setPackage( 'evndisplay' )
  #job.setVersion( 'prod3_d20150903b' )
  job.setVersion( 'prod3_d20150918' ) ### version with less verbose output

  # set EvnDisp Meta data
  job.setEvnDispMD( infileList[0] )

  # # set layout and telescope combination
  job.setLayoutList( "3HB1 3HB2 3HB3 3HD1 3HD2 3HI1" )
  job.setTelescopetypeCombinationList( "FA NA FG NG FD ND" )
  #  set calibration file and parameters file
  job.setCalibrationFile( 'prod3.peds.20150820.dst.root' )
  job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )
  

  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow()

  if mode == 'TS':
    res = submitTS( job, infileList )
  elif mode == 'WMS':
    res = submitWMS( job, infileList, 10 )
  else:
    Script.showHelp()
    
  # debug
    #Script.gLogger.info( job.workflow )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) < 1):
    Script.showHelp()
  try:
    res = runEvnDisp3( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

""" EvnDisp Script to create a Transformation
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s mode infile' % Script.scriptName,
                                     'Arguments:',
                                     '  infile: ascii file with input files LFNs',
                                     '\ne.g: %s Paranal_gamma_North.list' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from CTADIRAC.Interfaces.API.EvnDisp3Job import EvnDisp3Job
from DIRAC.Core.Workflow.Parameter import Parameter

def submitTS( job, infileList ):
  """ Create a transformation executing the job workflow  """
  t = Transformation()
  tc = TransformationClient()
  t.setType( "DataReprocessing" )
  t.setDescription( "EvnDisp3 example" )
  t.setLongDescription( "EvnDisplay analysis" )  # mandatory
  t.setGroupSize(3) # 3 for protons and 1 for electrons and gamma
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

##################################
  job = EvnDisp3Job(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  # override for testing
  job.setName( 'EvnDisp3' )
  ## add Type for JobByType mapping
  job.setType('EvnDisp3')
   
  # package and version
  job.setPackage( 'evndisplay' )
  job.setVersion( 'prod3_d20170125' )
  
  # set EvnDisp Meta data
  job.setEvnDispMD( infileList[0] )

  # # set layout
  job.setPrefix( "CTA.prod3Nb" )
  job.setLayoutList( "3AL4-AF15 3AL4-AN15 3AL4-BF15 3AL4-BN15 3AL4-CF15 3AL4-CN15 3AL4-DF15 3AL4-DN15 3AL4-FF15 3AL4-FN15 3AL4-GF15 3AL4-GN15 3AL4-HF15 3AL4-HN15 hyperarray-F hyperarray-N")
  job.setCalibrationFile( 'gamma_20deg_180deg_run3___cta-prod3-lapalma3-2147m-LaPalma.ped.root' ) # for La Palma

  job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )
  
  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow()
  ### Need to be added here, since not automatically added
  job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "", True, False, "Temporary fix" ) )
  job.workflow.addParameter( Parameter( "PRODUCTION_ID", "000000", "string", "", "", True, False, "Temporary fix" ) )

  res = submitTS( job, infileList )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 1):
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

""" EvnDisp Script to create a Transformation with Input Data
"""

import json
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s transName' % Script.scriptName,
                                     'Arguments:',
                                     '  transName: name of the transformation',
                                     '\ne.g: %s evndisp-gamma-N' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from CTADIRAC.Interfaces.API.EvnDisp3RefJob import EvnDisp3RefJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter

def submitTS( job, transName, mqJson ):
  """ Create a transformation executing the job workflow  """
  DIRAC.gLogger.notice( 'submitTS' )

  # Initialize JOB_ID
  job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "", True, False, "Temporary fix" ) )
  
  tc = TransformationClient()

  res = tc.addTransformation( transName, 'EvnDisp3 example', 'EvnDisplay calib_imgreco', 'DataReprocessing', 'Standard', 'Automatic', mqJson, groupSize = 5, body = job.workflow.toXML() )

  if not res['OK']:
    DIRAC.gLogger.error ( res['Message'] )
    DIRAC.exit( -1 )
  else:
    transID = res['Value']
    print(transID)

  return res

#########################################################

def runEvnDisp3( args = None ):
  """ Simple wrapper to create a EvnDisp3RefJob and setup parameters
      from positional arguments given on the command line.

      Parameters:
      args -- infile mode
  """
  DIRAC.gLogger.notice( 'runEvnDisp3' )
  # get arguments
  transName = args[0]

  ################################
  job = EvnDisp3RefJob(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  # override for testing
  job.setName( 'EvnDisp3' )
  ## add for testing
  job.setType('EvnDisp3')

  # defaults
  # job.setLayout( "Baseline")
  ## package and version
  # job.setPackage( 'evndisplay' )
  # job.setVersion( 'prod3b_d20170602' )
  # job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  # job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )

  # change here for Paranal or La Palma
  job.setPrefix( "CTA.prod3Nb" )

  #  set calibration file and parameters file
  job.setCalibrationFile( 'gamma_20deg_180deg_run3___cta-prod3-lapalma3-2147m-LaPalma.ped.root' ) # for La Palma


  ### set meta-data to the product of the transformation
  # set query to add files to the transformation
  MDdict = {'MCCampaign':'PROD3', 'particle':'gamma', 'array_layout':'Baseline', 
            'site':'LaPalma', 'outputType':'Data',
            'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2017-04-19',
            'thetaP':{"=": 20}, 'phiP':{"=": 0.0}}
  job.setEvnDispMD( MDdict )

  # add the sequence of executables
  job.setTSTaskId( '@{JOB_ID}' ) # dynamic
  job.setupWorkflow(debug=True)

  # output
  job.setOutputSandbox( ['*Log.txt'] )

  ### submit the workflow to the TS
  res = submitTS( job, transName, json.dumps( MDdict ) )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 1 ):
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

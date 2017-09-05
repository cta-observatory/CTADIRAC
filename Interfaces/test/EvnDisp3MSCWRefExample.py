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
from CTADIRAC.Interfaces.API.EvnDisp3MSCWRefJob import EvnDisp3MSCWRefJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter

def submitTS( job, transName, mqJson ):
  """ Create a transformation executing the job workflow  """
  tc = TransformationClient()

  # Initialize JOB_ID
  job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "", True, False, "Temporary fix" ) )

  res = tc.addTransformation( transName, 'EvnDisp3MSCW example', 'EvnDisplay stereo reconstruction', 'DataReprocessing', 'Standard', 'Automatic', mqJson, groupSize = 10, body = job.workflow.toXML() )

  transID = res['Value']
  print  transID

  return res

#########################################################

def runEvnDisp3( args = None ):
  """ Simple wrapper to create a EvnDisp3RefJob and setup parameters
      from positional arguments given on the command line.

      Parameters:
      args -- infile mode
  """
  # get arguments
  transName = args[0]

  ################################
  job = EvnDisp3MSCWRefJob(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  # override for testing
  job.setName( 'EvnDisp3MSCW' )
  ## add for testing
  job.setType('EvnDisp3')

  # defaults
  # job.setLayout( "Baseline")
  ## package and version
  # job.setPackage( 'evndisplay' )
  # job.setVersion( 'prod3b_d20170602' )

  # change here for Paranal or La Palma
  job.setPrefix( "CTA.prod3Nb" )
  job.setPointing(180)
  job.setDispSubDir('BDTdisp.Nb.3AL4-BN15.T1')
  job.setRecId('0,1,2') # 0 = all teltescopes, 1 = LST only, 2 = MST only
  #  set calibration file and parameters file
  job.setTableFile( 'tables_CTA-prod3b-LaPalma-NNq05-NN-ID0_0deg-d20160925m4-Nb.3AL4-BN15.root' ) # for La Palma


  ### set meta-data to the product of the transformation
  # set query to add files to the transformation
  MDdict = {'MCCampaign':'PROD3', 'particle':'gamma', 'array_layout':'Baseline', \
            'site':'LaPalma', 'outputType':'Data',\
            'calibimgreco_prog':'evndisp', 'calibimgreco_prog_version':'prod3b_d20170602',\
            'thetaP':{"=": 20}, 'phiP':{"=": 180.0}}
  job.setEvnDispMD( MDdict )

  # add the sequence of executables
  job.setTSTaskId( '@{JOB_ID}' ) # dynamic
  job.setupWorkflow()

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

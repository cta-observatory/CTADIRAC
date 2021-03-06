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
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from CTADIRAC.Interfaces.API.EvnDisp3RefJob import EvnDisp3RefJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter

def submitTS( job, transName, mqJson ):
    """ Create a transformation executing the job workflow  """
    DIRAC.gLogger.notice( 'submitTS' )

    # Initialize JOB_ID
    job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "",
                                       True, False, "Temporary fix" ) )
   
    t = Transformation( )
    t.setTransformationName(transName) # this must be unique
    t.setType("DataReprocessing")
    t.setDescription("EvnDisplay MQ example")
    t.setLongDescription( "EvnDisplay calib_imgreco") # mandatory
    t.setBody (job.workflow.toXML())
    t.setGroupSize(5)
    t.setFileMask(mqJson) # catalog query is defined here
    t.addTransformation() # transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")

    return
    
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
  particle  = args[1]
  nsb_level = int(args[2])
  

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
  job.setVersion('prod3b_d20170922')
  job.configuration_id=1
  # job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  # job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )

  # change here for Paranal or La Palma
  job.setPrefix( "CTA.prod3Sb" )

  #  set calibration file and parameters file
  # pedestals_KB_NSB30x.root, pedestals_KB_NSB5x.root, pedestals_KB_NSB1x.root
  job.setCalibrationFile('pedestals_KB_NSB%dx.root'%nsb_level)


  ### set meta-data to the product of the transformation
  # set query to add files to the transformation
  MDdict = {'MCCampaign':'PROD3', 'particle': particle,
            'array_layout':'Baseline', 'site':'Paranal',
            'outputType':'Data', 'data_level':{"=": 0},
            'configuration_id':{"=": 1},
            'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2017-09-01',
            'thetaP':{"=": 20}, 'phiP':{"=": 180.0}, 'nsb':{"=": nsb_level}}

  job.setEvnDispMD(MDdict)

  # output file metadata
  job.filemetadata={'nsb':nsb_level}
  # add the sequence of executables
  job.setTSTaskId( '@{JOB_ID}' ) # dynamic
  job.setupWorkflow(debug=False)

  # output
  job.setOutputSandbox( ['*Log.txt'] )

  ### submit the workflow to the TS
  res = submitTS( job, transName, json.dumps( MDdict ) )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 3 ):
    Script.showHelp()
  
  runEvnDisp3(args)

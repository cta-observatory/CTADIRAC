""" Prod3 MC Pipe Script to create a Transformation
          JB, LA December 2016
"""

import json
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s array_layout site particle pointing_dir zenith_angle nShower' % Script.scriptName,
                                     'Arguments:',
                                     '  array_layout: hex or square',
                                     '  site: Paranal, Aar, Armazones_2K',
                                     '  particle: gamma, proton, electron',
                                     '  pointing_dir: North or South',
                                     '  zenith_agle: 20',
                                     '  nShower: from 5 to 25000',
                                     '\ne.g: %s hex Paranal gamma South 20 5'% Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.Core.Workflow.Parameter import Parameter
from CTADIRAC.Interfaces.API.Prod3MCPipeJob import Prod3MCPipeJob
from CTADIRAC.Interfaces.API.EvnDisp3JobID import EvnDisp3JobID
from CTADIRAC.ProductionSystem.Client.ProductionClient import ProductionClient

def runProduction():

  prodClient = ProductionClient()

  jobMC1 = defineProd3MCJob( '2016-12-20c', 'LaPalma3', 'LaPalma',  'gamma', 'North', 20, 5 )
  transMC1 = defineTrans( 'transMC1', 'MC prod3', 'MC prod3 with corsika and simtel', 'MCSimulation', 'Standard', 'Automatic', jobMC1 )
  prodClient.addTransformation( transMC1 )

  jobMC2 = defineProd3MCJob( '2016-12-20c', 'LaPalma3', 'LaPalma',  'proton', 'North', 20, 5 )
  transMC2 = defineTrans( 'transMC2', 'MC prod3', 'MC prod3 with corsika and simtel', 'MCSimulation', 'Standard', 'Automatic', jobMC2 )
  prodClient.addTransformation( transMC2 )

  jobAna = defineEvnDisp3Job( 'prod3_d20170125', "3AL4-AF15 3AL4-AN15 3AL4-BF15 3AL4-BN15 3AL4-CF15 3AL4-CN15 3AL4-DF15 3AL4-DN15 3AL4-FF15 3AL4-FN15 3AL4-GF15 3AL4-GN15 3AL4-HF15 3AL4-HN15 hyperarray-F hyperarray-N" )
  MDdict = {'MCCampaign':'PROD3', 'particle':'proton', 'array_layout':'LaPalma3', 'site':'LaPalma', 'outputType':'Data', 'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2016-12-20c', 'thetaP':{"=": 20}, 'phiP':{"=": 180.0}}
  transAna = defineTrans( 'transAna', 'EvnDisp3', 'EvnDisplay analysis', 'DataReprocessing', 'Standard', 'Automatic', jobAna, FileMask = json.dumps( MDdict ), groupSize = 1 )
  prodClient.addTransformation( transAna )

  # Create the production and start it
  prodClient.addProduction()
  prodClient.setStatus( 'Active' )

  return 0

def defineTrans( Name, Description, LongDescription, Type, Plugin, AgentType, job, FileMask = '', groupSize = 1):
  """ Create a transformation executing the job workflow  """

  t = Transformation()
  t.setTransformationName( Name )  # This must be unique. If not set it's asked in the prompt
  t.setType( Type )
  t.setDescription( Description )
  t.setLongDescription( LongDescription )  # mandatory
  t.setAgentType ( AgentType )
  t.setPlugin ( Plugin )
  t.setGroupSize( groupSize )
  t.setBody ( job.workflow.toXML() )
  t.setFileMask( FileMask )

  return t

#########################################################
def defineEvnDisp3Job( version, LayoutList ):
  """ Simple wrapper to create a Prod3MCJob and setup parameters
  """

  job = EvnDisp3JobID(cpuTime = 432000)
  # package and version
  job.setType('EvnDisp3')

  # package and version
  job.setPackage( 'evndisplay' )
  job.setVersion( version ) ### for La Palma optimized

  # set query to add files to the transformation
  MDdict = {'MCCampaign':'PROD3', 'particle':'proton', 'array_layout':'LaPalma3', 'site':'LaPalma', 'outputType':'Data', 'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2016-12-20c', 'thetaP':{"=": 20}, 'phiP':{"=": 180.0}}

  ### set meta-data to the product of the transformation
  job.setEvnDispMD( MDdict )

  # # set layout and telescope combination
  job.setPrefix( "CTA.prod3Nb" )
  job.setLayoutList( LayoutList ) # two new layouts
  #  set calibration file and parameters file
  job.setCalibrationFile( 'gamma_20deg_180deg_run3___cta-prod3-lapalma3-2147m-LaPalma.ped.root' ) # for La Palma
  job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )
  job.setOutputSandbox( ['*Log.txt'] )
  # add the sequence of executables
  job.setupWorkflow()

  return job

def defineProd3MCJob( version, layout, site, particle, pointing, zenith, nShower ):
  """ Simple wrapper to create a Prod3MCJob and setup parameters
  """

  job = Prod3MCPipeJob()
  # package and version
  job.setPackage('corsika_simhessarray')
  job.setVersion( version )  # final with fix for gamma-diffuse
  job.no_sct=True # NO SCT for 40deg !
  job.setArrayLayout( layout )
  job.setSite( site )
  job.setParticle( particle )
  job.setPointingDir( pointing )
  job.setZenithAngle( zenith )
  job.setNShower( nShower )
  ### Set the startrunNb here (it will be added to the Task_ID)
  startrunNb = '0'
  job.setStartRunNumber( startrunNb )
  # set run number for TS submission: JOB_ID variable left for dynamic resolution during the Job. It corresponds to the Task_ID
  job.setRunNumber( '@{JOB_ID}' )
  # get dirac log files
  job.setOutputSandbox( ['*Log.txt'] )
  # add the sequence of executables
  job.setupWorkflow(debug=False)

  ### Temporary fix to initialize JOB_ID #######
  job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "", True, False, "Temporary fix" ) )
  job.workflow.addParameter( Parameter( "PRODUCTION_ID", "000000", "string", "", "", True, False, "Temporary fix" ) )
  job.setType('MCSimulation') ## Used for the JobType plugin

  return job

#########################################################
if __name__ == '__main__':

  try:
    res = runProduction()
    if res:
      DIRAC.gLogger.error ( 'Error in runProduction' )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

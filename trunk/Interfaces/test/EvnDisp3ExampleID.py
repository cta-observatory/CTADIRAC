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
from CTADIRAC.TransformationSystem.Client.TransformationClient import TransformationClient 
from CTADIRAC.Interfaces.API.EvnDisp3JobID import EvnDisp3JobID
from DIRAC.Interfaces.API.Dirac import Dirac

def submitTS( job, transName, mqJson ):
  """ Create a transformation executing the job workflow  """
  tc = TransformationClient()

  res = tc.addTransformation( transName, 'EvnDisp3 example', 'EvnDisplay analysis', 'DataReprocessing', 'Standard', 'Automatic', mqJson, groupSize = 1, body = job.workflow.toXML() )

  transID = res['Value']
  print  transID

  return res

#########################################################

def runEvnDisp3( args = None ):
  """ Simple wrapper to create a EnDisp3Job and setup parameters
      from positional arguments given on the command line.
      
      Parameters:
      args -- infile mode
  """
  # get arguments
  transName = args[0]

  ################################
  job = EvnDisp3JobID(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###
  # override for testing
  job.setName( 'EvnDisp3' )
  ## add for testing
  job.setType('EvnDisp3')
    
  # package and version
  job.setPackage( 'evndisplay' )
  job.setVersion( 'prod3_d20170125' ) ### for La Palma optimized
  
  # set query to add files to the transformation
  MDdict = {'MCCampaign':'PROD3', 'particle':'proton', 'array_layout':'LaPalma3', 'site':'LaPalma', 'outputType':'Data', 'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2016-12-20c', 'thetaP':{"=": 20}, 'phiP':{"=": 180.0}}

  ### set meta-data to the product of the transformation
  job.setEvnDispMD( MDdict )

  # # set layout and telescope combination
  job.setPrefix( "CTA.prod3Nb" )
  job.setLayoutList( "3AL4-AF15 3AL4-AN15 3AL4-BF15 3AL4-BN15 3AL4-CF15 3AL4-CN15 3AL4-DF15 3AL4-DN15 3AL4-FF15 3AL4-FN15 3AL4-GF15 3AL4-GN15 3AL4-HF15 3AL4-HN15 hyperarray-F hyperarray-N") # two new layouts
  #  set calibration file and parameters file
  job.setCalibrationFile( 'gamma_20deg_180deg_run3___cta-prod3-lapalma3-2147m-LaPalma.ped.root' ) # for La Palma
  
  job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )
  
  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow()
  ### submit the workflow to the TS
  mqJson = json.dumps( MDdict )

  res = submitTS( job, transName, mqJson )
   
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

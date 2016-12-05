""" EvnDisp Script to create a Transformation with Input Data
    for HB9 SCT analysis
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s infile' % Script.scriptName,
                                     'Arguments:',
                                     ' infile: ascii file with input files LFNs',
                                     '\ne.g: %s Paranal_gamma_North_HB9.list' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from CTADIRAC.TransformationSystem.Client.TransformationClient import TransformationClient 
from CTADIRAC.Interfaces.API.EvnDisp3JobIDSCT import EvnDisp3JobIDSCT
from DIRAC.Interfaces.API.Dirac import Dirac


#########################################################
# New SubmitTS - not obvious to make a query with multiple input files
#def submitTS( job, transName, mqJson ):
#  """ Create a transformation executing the job workflow  """
#  tc = TransformationClient()
#  
#  # here groupSize = 5
#  groupSize = 5
#  res = tc.addTransformation( transName, 'EvnDisp3 example', 'EvnDisplay analysis',\
#                              'DataReprocessing', 'Standard', 'Automatic', mqJson,\
#                              groupSize = groupSize, body = job.workflow.toXML() )
#  transID = res['Value']
#  print  transID
#  return res

#########################################################
# OldSubmit to TS
def OldSubmitTS( job, infileList ):
  """ Create a transformation executing the job workflow  """
  t = Transformation()
  tc = TransformationClient()
  t.setType( "DataReprocessing" )
  t.setDescription( "Runs EvnDisp analysis for array HB9 SCT" )
  t.setLongDescription( "merge_simtel, evndisp converter and evndisp analysis for HB9 SCT" )  # mandatory
  t.setGroupSize(5)
  t.setBody ( job.workflow.toXML() )

  res = t.addTransformation()  # Transformation is created here

  if not res['OK']:
    print res['Message']
    DIRAC.exit( -1 )

  t.setStatus( "Active" )
  t.setAgentType( "Automatic" )
  transID = t.getTransformationID()
  print('Adding %s files to transformation'%len(infileList))
  tc.addFilesToTransformation( transID['Value'], infileList )  # Files added here

  return res

#########################################################
def submitWMS( job, infileList, nbFileperJob ):
  """ Submit the job locally or to the WMS  """

  dirac = Dirac()
  res = Dirac().splitInputData( infileList, nbFileperJob )
  if not res['OK']:
    Script.gLogger.error( 'Failed to splitInputData' )
    DIRAC.exit( -1 )

#  job.setGenericParametricInput( res['Value'] )
  job.setParametricInputData( res['Value'] )
#  job.setInputData( '%s' )
 
  job.setJobGroup( 'EvnDisp3-SCT-test' )

  job.setInputSandbox( ['cta-prod3-get-matching-data.py'] )   

  res = dirac.submit( job )

  Script.gLogger.notice( 'Submission Result: ', res )
  return res
  
  
########################################################
def runEvnDisp3IDSCT( args = None ):
  """ Simple wrapper to create a EvnDisp3JobIDSCT and setup parameters
      from positional arguments given on the command line.
      
      Parameters:
      args -- infile mode
  """
  # get arguments
  infile = args[0]
  
  # debug with WMS - list of input file names
  f = open( infile, 'r' )
  infileList = []
  for line in f:
    infile = line.strip()
    if line != "\n":
      infileList.append( infile )

  ################################
  job = EvnDisp3JobIDSCT(cpuTime = 36000)  # to be adjusted!!

  ### Main Script ###
  # override for testing
  job.setName( 'EvnDisp3SCT' )
  ## add for testing
  job.setType('DataReprocessing')
    
  # package and version
  job.setPackage( 'evndisplay' )
  job.setVersion( 'prod3bSCT_d20161130' ) ## HB9-SCT

  # set EvnDisp Meta data
  job.setEvnDispMD( infileList[0] )

  # set query to add files to the transformation
  #  MDdict = {'MCCampaign':'PROD3', 'particle':'gamma', 'array_layout':'full',\
  #            'site':'Paranal', 'outputType':'Data', 'tel_sim_prog':'simtel',\
  #            'thetaP':{"=": 20}, 'phiP':{"=": 0.0}, 'sct':'False'}
  
  ### set meta-data to the product of the transformation ( overrides sct to true)
  #  job.setEvnDispMD( MDdict )
  
  # # set layout and telescope combination
  #job.setPrefix( "CTA.prod3Sb" ) 
  #job.setLayoutList( "3HB1-2 3HB2-2 3HB4-2 3HB8 3HB89 3HB9" ) 
  #job.setTelescopetypeCombinationList( "FA FD FG NA ND NG" )
  #  set calibration file and parameters file
  #job.setCalibrationFile( 'gamma_20deg_180deg_run5___cta-prod3-demo_desert-2150m-Paranal.ped.root' )
  #job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  #job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )
  
  # set sandbox
  job.setOutputSandbox( ['*Log.txt'] )

  # add the sequence of executables
  job.setupWorkflow(debug=True)

  # submit to the Transformation System
  res = OldSubmitTS( job, infileList[:10] )

  # or to the WMS for debug
  #res = submitWMS(job, infileList[:4], 2)
  # debug
  Script.gLogger.info( job.workflow )

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if ( len( args ) != 1 ):
    Script.showHelp()
  try:
    res = runEvnDisp3IDSCT( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

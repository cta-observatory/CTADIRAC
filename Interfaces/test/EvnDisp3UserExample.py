""" Prod3 EvnDisp Script for users
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s infile' % Script.scriptName,
                                     'Arguments:',
                                     '  infile: ascii file with input files LFNs',
                                     '\ne.g: %s Paranal_gamma_North.list' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from CTADIRAC.Interfaces.API.EvnDisp3UserJob import EvnDisp3UserJob
from DIRAC.Interfaces.API.Dirac import Dirac


def submitWMS( job, infileList ):
  """ Submit the job to the WMS  """

  dirac = Dirac()

  job.setParametricInputData( infileList )
  job.setOutputData( ['./*evndisp.tar.gz'] ) # to be used if DataManagement step in EvnDisp3UserJob is commented

  #job.setJobGroup( 'EvnDisp-proton' )
  job.setName( 'evndispjob' )
  job.setOutputSandbox( ['*Log.txt'] )
  #job.setInputSandbox( ['myconf'] )

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

##################################
  job = EvnDisp3UserJob(cpuTime = 432000)  # to be adjusted!!

  ### Main Script ###

  # package and version
  job.setPackage( 'evndisplay' )
  job.setVersion( 'prod3_d20160111' )

  # # set layout list
  job.setLayoutList( "CTA.prod3S.3HB1-NA.lis CTA.prod3S.3HB2-NA.lis" )
  #  set calibration file and parameters file
  job.setCalibrationFile( 'ped.20151106.evndisp.root' )
  job.setReconstructionParameter( 'EVNDISP.prod3.reconstruction.runparameter.NN' )
  job.setNNcleaninginputcard( 'EVNDISP.NNcleaning.dat' )
  
  # add the sequence of executables
  job.setupWorkflow()

  res = submitWMS( job, infileList )

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
      DIRAC.gLogger.notice( res['Value'] )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

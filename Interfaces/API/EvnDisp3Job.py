"""
  Simple Wrapper on the Job class to handle EvnDisp Analysis
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job

class EvnDisp3Job( Job ) :
  """ Job extension class for EvnDisp Analysis,
      takes care of running converter and evndisp.
  """
        
  def __init__( self, cpuTime = 432000 ):
    """ Constructor
    
    Keyword arguments:
    cpuTime -- max cpu time allowed for the job
    """
    Job.__init__( self )
    self.setCPUTime( cpuTime )
    # defaults
    self.setName('Evndisplay_Analysis')
    self.package='evndisplay'
    self.version='v500-prod3v1'
    self.calibration_file = 'prod3.peds.20150820.dst.root'
    self.reconstructionparameter = 'EVNDISP.prod3.reconstruction.runparameter.NN'
    self.NNcleaninginputcard = 'EVNDISP.NNcleaning.dat'

  def setPackage(self, package):
    """ Set package name : e.g. 'evndisplay'
    
    Parameters:
    package -- evndisplay
    """
    self.package=package

  def setVersion(self, version):
    """ Set software version number : e.g. v500-prod3v1
    
    Parameters:
    version -- evndisplay package version number
    """
    self.version=version
    
  def setCalibrationFile( self, calibration_file ):
    """ Set the calibration file
    
    Parameters:
    calibration_file -- calibration file name
    """
    self.calibration_file = calibration_file

  def setReconstructionParameter( self, reconstructionparameter ):
    """ Set the reconstruction parameter file
    
    Parameters:
    reconstructionparameter -- parameter file for reconstruction
    """
    self.reconstructionparameter = reconstructionparameter
    
  def setNNcleaninginputcard( self, NNcleaninginputcard ):
    """ Set the cleaning inputcard
    
    Parameters:
    NNcleaninginputcard -- cleaning inputcard
    """
    self.NNcleaninginputcard = NNcleaninginputcard


  def setupWorkflow(self, debug=False):
    """ Setup job workflow by defining the sequence of all executables
        All parameters shall have been defined before that method is called.
    """
    # step 1 -- to be removed -- debug only
    iStep = 1
    if debug:
        lsStep = self.setExecutable( '/bin/ls -alhtr', logFile = 'LS_Init_Log.txt' )
        lsStep['Value']['name']='Step%i_LS_Init'%iStep
        lsStep['Value']['descr_short']='list files in working directory'
        iStep+=1
      
    # step 2  
    lsStep = self.setExecutable( '$DIRACROOT/scripts/cta-evndisp-setupsw',
                              arguments='%s %s'% (self.package, self.version),\
                              logFile='SetupSoftware_Log.txt')
    lsStep['Value']['name']='Step%i_SetupSoftware'%iStep
    lsStep['Value']['descr_short'] = 'Setup software'
    iStep+=1
    
    for subarray in range( 1, 6 ):
      # step 3  Need to decide which arguments are passed here and which are hard-coded in the shell script
      csStep = self.setExecutable( './dirac_evndisp', \
                                arguments = '--subarray %s --calibration_file %s --reconstructionparameter %s --NNcleaninginputcard %s' % ( subarray, self.calibration_file, self.reconstructionparameter, self.NNcleaninginputcard ), \
                                logFile = 'EvnDisp_Log.txt' )
      csStep['Value']['name'] = 'Step%i_EvnDispConverter' % iStep
      csStep['Value']['descr_short'] = 'Run EvnDisplay'
      iStep += 1

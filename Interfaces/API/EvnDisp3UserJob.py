"""
  Simple Wrapper on the Job class to handle EvnDisp Analysis
"""

__RCSID__ = "$Id$"

# generic imports
import json
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job

class EvnDisp3UserJob( Job ) :
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
    self.version = 'prod3_d20150831b'
    self.layout_list = '3HB1 3HB2 3HB3 3HD1 3HD2 3HI1'
    self.calibration_file = 'prod3.peds.20150820.dst.root'
    self.reconstructionparameter = 'EVNDISP.prod3.reconstruction.runparameter.NN'
    self.NNcleaninginputcard = 'EVNDISP.NNcleaning.dat'
    ### To be defined if using step 4 to put and register files
    #self.outputpattern = './*evndisp.tar.gz'
    #self.outputpath = '/vo.cta.in2p3.fr/user/a/arrabito'
    #self.outputSE = json.dumps(['CC-IN2P3-USER','DESY-ZN-USER'])

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

  def setLayoutList( self, layout_list ):
    """ Set the layout list

    Parameters:
    layout_list -- list of layouts
    """
    self.layout_list = layout_list

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

    # step 1 : setup software
    iStep = 1
    swStep = self.setExecutable( 'cta-prod3-setupsw',
                              arguments='%s %s'% (self.package, self.version),\
                              logFile='SetupSoftware_Log.txt')
    swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
    swStep['Value']['descr_short'] = 'Setup software'
    iStep+=1

    # step 2: check input file size
    # arguments are nbFiles=0 (not used) and fileSize=100kB
    eivStep = self.setExecutable( '$cta-prod3-verifysteps', \
                              arguments = 'analysisinputs 0 100', \
                              logFile = 'Verify_EvnDispInputs_Log.txt' )
    eivStep['Value']['name'] = 'Step%i_VerifyEvnDispInputs' % iStep
    eivStep['Value']['descr_short'] = 'Verify EvnDisp Inputs'
    iStep += 1

    # step 3: run evendisplay
    evStep = self.setExecutable( './dirac_prod3_user_evndisp', \
                                arguments = "--layout_list '%s' --calibration_file %s --reconstructionparameter %s --NNcleaninginputcard %s" % \
                                            ( self.layout_list, self.calibration_file, self.reconstructionparameter, self.NNcleaninginputcard), \
                                logFile = 'EvnDisp_Log.txt' )
    evStep['Value']['name'] = 'Step%i_EvnDisplay' % iStep
    evStep['Value']['descr_short'] = 'Run EvnDisplay'
    iStep += 1

    # step 4
    # ## put and register files (to be used in replacement of setOutputData of Job API)

    #dmStep = self.setExecutable( '$DIRACROOT/CTADIRAC/Core/scripts/cta-user-managedata.py',
    #                          arguments = "'%s' %s '%s'" % ( self.outputpattern, self.outputpath, self.outputSE ),
    #                          logFile = 'DataManagement_Log.txt' )
    #dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    #dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
    #iStep += 1

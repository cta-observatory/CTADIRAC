"""
  Simple Wrapper on the Job class to handle EvnDisp calibration
  and image analysis (single telescope reconstruction
  Converter and EvnDisp for DL0 to DL1
  Specialized for Reference setup simulation with the Baseline layout
"""

__RCSID__ = "$Id$"

# generic imports
import json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class EvnDisp3RefJob( Job ) :
  """ Job extension class for EvnDisp Calibration and Reconstruction,
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
    self.setName('Evndisplay_CalibReco')
    self.package='evndisplay'
    self.program_category='calibimgreco'
    self.version = 'prod3b_d20170602' # or later
    self.configuration_id = 0
    self.output_data_level=1
    self.prefix = 'CTA.prod3Nb'
    self.layout = 'Baseline'
    self.calibration_file = 'gamma_20deg_180deg_run3___cta-prod3-lapalma3-2147m-LaPalma.ped.root'
    self.reconstructionparameter = 'EVNDISP.prod3.reconstruction.runparameter.NN'
    self.NNcleaninginputcard = 'EVNDISP.NNcleaning.dat'
    self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
    self.fcc = FileCatalogClient()
    self.metadata = collections.OrderedDict()
    self.filemetadata = {}
    self.jobGroupID = 1
    self.catalogs = ["DIRACFileCatalog","TSCatalog"]

  def setTSTaskId(self, taskid):
    """ Set TS task Id, dynamically resolved at job run time

    Parameters:
    taskid -- an int
    """
    self.ts_task_id=taskid

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

  def setPrefix( self, prefix ):
    """ Set prefix for layout name

    Parameters:
    prefix -- prefix for layout names
    """
    self.prefix = prefix

  def setLayout( self, layout ):
    """ Set the layout list

    Parameters:
    layout -- the layout "Baseline"
    """
    self.layout = layout

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

  def setEvnDispMD( self,  simtelMD ):
    """ Set evndisplay meta data

    Parameters:
    simtelMD -- metadata dictionary as for simtel
    """
    # # Set evndisp directory meta data
    self.metadata['array_layout'] = simtelMD['array_layout']
    self.metadata['site'] = simtelMD['site']
    self.metadata['particle'] = simtelMD['particle']
    self.metadata['phiP'] = simtelMD['phiP']['=']
    self.metadata['thetaP'] = simtelMD['thetaP']['=']
    self.metadata[self.program_category+'_prog'] = 'evndisp'
    self.metadata[self.program_category+'_prog_version'] = self.version
    self.metadata['data_level'] = self.output_data_level
    self.metadata['configuration_id'] = self.configuration_id

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
    swStep = self.setExecutable( '$DIRACROOT/scripts/cta-prod3-setupsw',
                              arguments='%s %s'% (self.package, self.version),\
                              logFile='SetupSoftware_Log.txt')
    swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
    swStep['Value']['descr_short'] = 'Setup software'
    iStep+=1

    # step 3
    # arguments are nbFiles=0 (not used) and fileSize=100kB
    eivStep = self.setExecutable( '$DIRACROOT/scripts/cta-prod3-verifysteps', \
                              arguments = 'analysisinputs 0 100', \
                              logFile = 'Verify_EvnDispInputs_Log.txt' )
    eivStep['Value']['name'] = 'Step%i_VerifyEvnDispInputs' % iStep
    eivStep['Value']['descr_short'] = 'Verify EvnDisp Inputs'
    iStep += 1

    # step 4
    evStep = self.setExecutable( './dirac_prod3_evndisp_ref', \
                                arguments = "--prefix %s --layout '%s' \
                                --calibration_file %s --reconstructionparameter %s \
                                --NNcleaninginputcard %s --taskid %s" % \
                                ( self.prefix, self.layout, self.calibration_file,
                                 self.reconstructionparameter, self.NNcleaninginputcard,
                                 self.ts_task_id), \
                                logFile = 'EvnDisp_Log.txt' )
    evStep['Value']['name'] = 'Step%i_EvnDisplay' % iStep
    evStep['Value']['descr_short'] = 'Run EvnDisplay'
    iStep += 1

    # step 5
    # ## the order of the metadata dictionary is important, since it's used to build the directory structure
    mdjson = json.dumps( self.metadata )
    metadatafield = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)',
                     'particle':'VARCHAR(128)', 'phiP':'float',
                     'thetaP': 'float',
		          self.program_category+'_prog':'VARCHAR(128)',
		          self.program_category+'_prog_version':'VARCHAR(128)',
                     'data_level': 'int', 'configuration_id': 'int'}
    mdfieldjson = json.dumps(metadatafield)

    # register Data
    outputpattern = './Data/*DL%01d.root'%self.output_data_level
    filemetadata = {}
    file_md_json = json.dumps(filemetadata)
    dmStep = self.setExecutable('$DIRACROOT/CTADIRAC/Core/scripts/cta-analysis-managedata.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s'" %\
                              (mdjson, mdfieldjson, file_md_json, self.basepath,
                               outputpattern, self.package, self.program_category, self.catalogs),
                              logFile = 'DataManagement_Log.txt')
    dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
    iStep += 1

    # register Log
    outputpattern = './*.logs.tgz'
    filemetadata = {}
    file_md_json = json.dumps(filemetadata)
    dmStep = self.setExecutable('$DIRACROOT/CTADIRAC/Core/scripts/cta-analysis-managedata.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s' Log" % \
                              (mdjson, mdfieldjson, file_md_json, self.basepath,
                               outputpattern, self.package, self.program_category, self.catalogs),
                              logFile = 'Log_DataManagement_Log.txt')
    dmStep['Value']['name'] = 'Step%i_Log_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save log files to SE and register them in DFC'
    iStep += 1

    # step 6 -- to be removed -- debug only
    if debug:
        lsStep = self.setExecutable('/bin/ls',
                                    arguments = " -alhtr; /bin/ls -lahrt ./Data",
                                    logFile = 'LS_End_Log.txt')
        lsStep['Value']['name']='Step%i_LS_End'%iStep
        lsStep['Value']['descr_short']='list files in working directory and in Data directory'
        iStep+=1

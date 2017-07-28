"""
  Simple Wrapper on the Job class to handle EvnDisp stereo reconstruction
  runs mscw_energy for DL1 to DL2 using look up tables
  Specialized for Reference setup simulation with the Baseline layout
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class EvnDisp3MSCWRefJob( Job ) :
  """ Job extension class for EvnDisp Analysis,
      takes care of running mscw_energy.
  """

  def __init__( self, cpuTime = 432000 ):
    """ Constructor

    Keyword arguments:
    cpuTime -- max cpu time allowed for the job
    """
    Job.__init__( self )
    self.setCPUTime( cpuTime )
    # defaults
    self.setName('Evndisplay_Reco')
    self.package='evndisplay'
    self.version = 'prod3b_d20170602' # or later
    self.prefix = 'CTA.prod3Nb'
    self.layout = 'Baseline'
    self.pointing = '180'
    self.table_file='tables_CTA-prod3b-LaPalma-NNq05-NN-ID0_0deg-d20160925m4-Nb.3AL4-BN15.root'
    self.disp_subdir_name = 'BDTdisp.Nb.3AL4-BN15.T1'
    self.recid  = '0,1,2' # 0 = all teltescopes, 1 = LST only, 2 = MST only
    self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
    self.fcc = FileCatalogClient()
    self.metadata = collections.OrderedDict()
    self.filemetadata = {}
    self.jobGroupID = 10

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

  def setPointing( self, angle ):
    """ Set the pointing direction for reconstruction
    @todo could be made North or South ?

    Parameters:
    pointing -- an angle in degrees 0 or 180
    """
    self.pointing = angle

  def setTableFile( self, table ):
    """ Set the table file name used for reconstruction
    This is the look up table for direction and energy
    File will be look for in directory "$CTA_EVNDISP_AUX_DIR/Tables/"

    Parameters:
    table -- lookup table file name
    """
    self.table_file = table

  def setDispSubDir( self, dispdir ):
    """ Set the BDT directory name

    Parameters:
    dispdir -- sub-directory name
    """
    self.disp_subdir_name = dispdir

  def setRecId( self, recid ):
    """ Set the list of telescopes sub-layout to reconstruction
    For North site (La Palma):
    + 0 = full array, LSTs+MSTs
    + 1 = 4 LSTs only
    + 2 = 15 MSTs only

    Parameters:
    recid -- a coma separated list of ints (0,1,2)
    """
    self.recid = recid

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
    self.metadata['reconstruction_prog'] = 'evndisp'
    self.metadata['reconstruction_prog_version'] = self.version

    # ## Set file metadata
    # self.filemetadata = {'runNumber': simtelMD['runNumber']}

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
    evStep = self.setExecutable( './dirac_prod3_evndisp_mscw_ref', \
        arguments = "--prefix %s --layout '%s' --pointing %s --table_file %s\
                     --disp_subdir_name %s --recid '%s'" %\
                    ( self.prefix, self.layout, self.pointing, self.table_file,\
                      self.disp_subdir_name, self.recid),\
        logFile = 'EvnDisp_MSCW_Log.txt' )
    evStep['Value']['name'] = 'Step%i_EvnDisplay_MSMCW' % iStep
    evStep['Value']['descr_short'] = 'Run EvnDisplay mscw_energy'
    iStep += 1

    # step 5
    # ## the order of the metadata dictionary is important, since it's used to build the directory structure
    mdjson = json.dumps( self.metadata )
    metadatafield = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)', 'particle':'VARCHAR(128)', \
                         'phiP':'float', 'thetaP': 'float', 'reconstruction_prog':'VARCHAR(128)', 'reconstruction_prog_version':'VARCHAR(128)'}
    mdfieldjson = json.dumps( metadatafield )
    fmdjson = json.dumps( self.filemetadata )

    # register Data
    outputpattern = './Data/*-evndisp-DL2.root'
    dmStep = self.setExecutable( '$DIRACROOT/CTADIRAC/Core/scripts/cta-analysis-managedata2.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s" % ( mdjson, mdfieldjson, fmdjson, self.basepath, outputpattern, self.package ),
                              logFile = 'DataManagement_Log.txt' )
    dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
    iStep += 1

    # register Log
    self.outputpattern = './*.logs.tgz'
    dmStep = self.setExecutable( '$DIRACROOT/CTADIRAC/Core/scripts/cta-analysis-managedata2.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s Log" % ( mdjson, mdfieldjson, fmdjson, self.basepath, outputpattern, self.package ),
                              logFile = 'Log_DataManagement_Log.txt' )
    dmStep['Value']['name'] = 'Step%i_Log_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save log files to SE and register them in DFC'
    iStep += 1

    # step 6 -- to be removed -- debug only
    if debug:
        lsStep = self.setExecutable( '/bin/ls -alhtr; /bin/ls -lahrt ./Data', logFile = 'LS_End_Log.txt' )
        lsStep['Value']['name']='Step%i_LS_End'%iStep
        lsStep['Value']['descr_short']='list files in working directory and in Data directory'
        iStep+=1

    # TS Task Id as an environment variable
    self.setExecutionEnv( {'TS_TASK_ID' : '%s' % self.ts_task_id} )

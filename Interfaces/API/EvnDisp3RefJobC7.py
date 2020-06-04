"""
  Simple Wrapper on the Job class to handle EvnDisp calibration
  and image analysis (single telescope reconstruction
  Converter and EvnDisp for DL0 to DL1
  Specialized for Reference setup simulation with the Baseline layout
"""

__RCSID__ = "377d54b (2018-01-09 13:46:34 +0100) BREGEON Johan <johan.bregeon@lupm.in2p3.fr>"

# generic imports
import json
import collections
# DIRAC imports
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import DATA_LEVEL_METADATA_ID

class EvnDisp3RefJobC7( Job ) :
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
    self.output_data_level = DATA_LEVEL_METADATA_ID['DL1']
    self.prefix = 'CTA.prod3Sb'
    self.layout = 'baseline'
    self.calibration_file = 'gamma_20deg_180deg_run3___cta-prod3-lapalma3-2147m-LaPalma.ped.root'
    self.rfile = 'CTA.prod3b.EffectiveFocalLength.dat'
    self.reconstructionparameter = 'EVNDISP.prod3.reconstruction.runparameter.NN.LL'
    self.base_path = '/vo.cta.in2p3.fr/MC/PROD3/'
    self.fcc = FileCatalogClient()
    self.metadata = collections.OrderedDict()
    self.filemetadata = {}
    self.catalogs = json.dumps(['DIRACFileCatalog','TSCatalog'])
    self.ts_task_id = 0
   
  def set_meta_data( self,  simtel_md ):
    """ Set evndisplay meta data

    Parameters:
    simtel_md -- metadata dictionary as for simtel
    """
    # # Set evndisp directory meta data
    self.metadata['array_layout'] = simtel_md['array_layout']
    self.metadata['site'] = simtel_md['site']
    self.metadata['particle'] = simtel_md['particle']
    try:
      phiP = simtel_md['phiP']['=']
    except:
      phiP = simtel_md['phiP']
    self.metadata['phiP'] = phiP
    try:
      thetaP = simtel_md['thetaP']['=']
    except:
      thetaP = simtel_md['thetaP']
    self.metadata['thetaP'] = thetaP
        
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
    swStep = self.setExecutable( 'cta-prod3-setupsw',
                              arguments='%s %s'% (self.package, self.version),\
                              logFile='SetupSoftware_Log.txt')
    swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
    swStep['Value']['descr_short'] = 'Setup software'
    iStep+=1

    # step 3
    # arguments are nbFiles=0 (not used) and fileSize=100kB
    eivStep = self.setExecutable( 'cta-prod3-verifysteps', \
                              arguments = 'analysisinputs 0 100', \
                              logFile = 'Verify_EvnDispInputs_Log.txt' )
    eivStep['Value']['name'] = 'Step%i_VerifyEvnDispInputs' % iStep
    eivStep['Value']['descr_short'] = 'Verify EvnDisp Inputs'
    iStep += 1

    # step 4 - debug only
    if debug:
        lsStep=self.setExecutable('/bin/ls -Ralhtr',logFile='LS_End_Log.txt')
        lsStep['Value']['name']='Step%i_LS_End'%iStep
        lsStep['Value']['descr_short']='list files in working directory and sub-directory'
        iStep += 1

    # step 5
    evStep = self.setExecutable( './dirac_prod3_evndisp_ref', \
                                arguments = "--prefix %s --layout %s \
                                --calibration_file %s --rfile %s --reconstructionparameter %s \
                                --taskid %s" % \
                                ( self.prefix, self.layout, self.calibration_file,
                                  self.rfile, self.reconstructionparameter,
                                 self.ts_task_id), \
                                logFile = 'EvnDisp_Log.txt' )
    evStep['Value']['name'] = 'Step%i_EvnDisplay' % iStep
    evStep['Value']['descr_short'] = 'Run EvnDisplay'
    iStep += 1

    # step 6
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
    file_md_json = json.dumps(self.filemetadata)
    scripts = '../CTADIRAC/Core/scripts/'
    dmStep = self.setExecutable(scripts + 'cta-analysis-managedata.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s'" %\
                              (mdjson, mdfieldjson, file_md_json, self.base_path,
                               outputpattern, self.package, self.program_category, self.catalogs),
                              logFile = 'DataManagement_Log.txt')
    dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
    iStep += 1

    # register Log
    outputpattern = './*.logs.tgz'
    filemetadata = {}
    file_md_json = json.dumps(filemetadata)
    scripts = '../CTADIRAC/Core/scripts/'
    dmStep = self.setExecutable(scripts + 'cta-analysis-managedata.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s' Log" % \
                              (mdjson, mdfieldjson, file_md_json, self.base_path,
                               outputpattern, self.package, self.program_category, self.catalogs),
                              logFile = 'Log_DataManagement_Log.txt')
    dmStep['Value']['name'] = 'Step%i_Log_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save log files to SE and register them in DFC'
    iStep += 1

 

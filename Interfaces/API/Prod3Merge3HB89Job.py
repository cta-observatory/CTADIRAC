"""
  Simple Wrapper on the Job class to handle merge_simtel of 3HB89 array
  from subarrays 1, 2 and 5
"""

__RCSID__ = "$Id$"

# generic imports
import json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class Prod3Merge3HB89Job( Job ) :
  """ Job extension class for special merging of array 3HB89,
      runs merge_simtel
  """
        
  def __init__( self, cpuTime = 432000 ):
    """ Constructor
    
    Keyword arguments:
    cpuTime -- max cpu time allowed for the job
    """
    Job.__init__( self )
    self.setCPUTime( cpuTime )
    # defaults
    self.setName('Merge3HB89')
    self.package='corsika_simhessarray'
    self.version = '2015-10-20-p4'
    self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
    self.outputpattern = ''
    self.fcc = FileCatalogClient()
    self.metadata = collections.OrderedDict()
    self.filemetadata = {}
    self.jobGroupID = 1

  def setPackage(self, package):
    """ Set package name : e.g. 'corsika_simhessarray'
    
    Parameters:
    package -- corsika_simhessarray
    """
    self.package=package

  def setVersion(self, version):
    """ Set software version number : e.g. v500-prod3v1
    
    Parameters:
    version -- evndisplay package version number
    """
    self.version=version
    
  def setReadCtaMD( self, path ):
    """ Set read_cta meta data starting from path metadata
    
    Parameters:
    path -- path from which get meta data
    """
    # # Get simtel meta data from path
    res = self.fcc.getFileUserMetadata( path )
    simtelMD = res['Value']

    # # Set read_cta directory meta data
    self.metadata['array_layout'] = simtelMD['array_layout']
    self.metadata['site'] = simtelMD['site']
    self.metadata['particle'] = simtelMD['particle']
    self.metadata['phiP'] = simtelMD['phiP']
    self.metadata['thetaP'] = simtelMD['thetaP']
    self.metadata['analysis_prog'] = 'merge_simtel'
    self.metadata['analysis_prog_version'] = self.version


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
    inStep = self.setExecutable( '$DIRACROOT/scripts/cta-prod3-verifysteps', \
                              arguments = 'analysisinputs 0 100', \
                              logFile = 'Verify_ReadCtaInputs_Log.txt' )
    inStep['Value']['name'] = 'Step%i_VerifyReadCtaInputs' % iStep
    inStep['Value']['descr_short'] = 'Verify ReadCta Inputs'
    iStep += 1

    # step 4 - download subarray-2 files corresponding to sub-1 input
#    rctaStep = self.setExecutable( '$DIRACROOT/scripts/cta-prod3-get-matching-data sub2sub5',\
    rctaStep = self.setExecutable( 'python ./cta-prod3-get-matching-data.py sub2sub5',\
                                logFile = 'Download_Files_Log.txt' )
    rctaStep['Value']['name'] = 'Step%i_Download_Files' % iStep
    rctaStep['Value']['descr_short'] = 'Download subarray-2 and 5 Files'
    iStep += 1

    # step 5- no arguments !
    rctaStep = self.setExecutable( './dirac_prod3_merge_simtel_3HB89',\
                                logFile = 'Merge_Simtel_Log.txt' )
    rctaStep['Value']['name'] = 'Step%i_Merge_Simtel' % iStep
    rctaStep['Value']['descr_short'] = 'Run Merge Simtel'
    iStep += 1

    # step 5
    # ## the order of the metadata dictionary is important, since it's used to build the directory structure
    mdjson = json.dumps( self.metadata )

    metadatafield = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)', 'particle':'VARCHAR(128)', \
                         'phiP':'float', 'thetaP': 'float', 'analysis_prog':'VARCHAR(128)', 'analysis_prog_version':'VARCHAR(128)'}

    mdfieldjson = json.dumps( metadatafield )

    fmdjson = json.dumps( self.filemetadata )

    ## Upload Data files
    self.outputpattern = './*Paranal-3HB89-NGFD*.simtel.gz.tar'
    dmStep = self.setExecutable( '$DIRACROOT/CTADIRAC/Core/scripts/cta-analysis-managedata.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s" % ( mdjson, mdfieldjson, fmdjson, self.basepath, self.outputpattern, self.package ),
                              logFile = 'Data_DataManagement_Log.txt' )
    dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
    iStep += 1

    # Upload Histogram files and use 'Log' as outputType
    self.outputpattern = './*Paranal-3HB89-NGFD*.merge_logs.tar'

    dmStep = self.setExecutable( '$DIRACROOT/CTADIRAC/Core/scripts/cta-analysis-managedata.py',
                              arguments = "'%s' '%s' '%s' %s '%s' %s %s" % ( mdjson, mdfieldjson, fmdjson, self.basepath, self.outputpattern, self.package, 'Log'),
                              logFile = 'Log_DataManagement_Log.txt' )
    dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
    iStep += 1

"""
  Simple Wrapper on the Job class to handle Chimp Analysis
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class ChimpJob( Job ) :
  """ Job extension class for Chimp Analysis,
      takes care of running chimp converter
  """
        
  def __init__( self, cpuTime = 432000 ):
    """ Constructor
    
    Keyword arguments:
    cpuTime -- max cpu time allowed for the job
    """
    Job.__init__( self )
    self.setCPUTime( cpuTime )
    # defaults
    self.setName( 'Chimp_Analysis' )
    self.package = 'chimp'
    self.version = 'prod3_xxx'
    self.par1 = '-100.'
    self.par2 = './'
    self.par3 = '0'
    self.par4 = '-staroutput'
    # self.telescopetype_combination_list = 'FA NA FG NG FD ND'
    # self.calibration_file = 'prod3.peds.20150820.dst.root'
    # self.reconstructionparameter = 'EVNDISP.prod3.reconstruction.runparameter.NN'
    # self.NNcleaninginputcard = 'EVNDISP.NNcleaning.dat'
    self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
    self.outputpattern = './*.root'
    self.fcc = FileCatalogClient()
    self.metadata = collections.OrderedDict()
    self.filemetadata = {}
    self.jobGroupID = 1

  def setPackage(self, package):
    """ Set package name : e.g. 'chimp'
    
    Parameters:
    package -- chimp
    """
    self.package=package

  def setVersion(self, version):
    """ Set software version number : e.g. v500-prod3v1
    
    Parameters:
    version -- chimp package version number
    """
    self.version=version
    
  '''def setNNcleaninginputcard( self, NNcleaninginputcard ):
    """ Set the cleaning inputcard
    
    Parameters:
    NNcleaninginputcard -- cleaning inputcard
    """
    self.NNcleaninginputcard = NNcleaninginputcard'''
    
  def setMD( self, path ):
    """ Set chimp meta data starting from path metadata
    
    Parameters:
    path -- path from which get meta data
    """
    # # Get simtel meta data from path
    res = self.fcc.getFileUserMetadata( path )
    simtelMD = res['Value']

    # # Set evndisp directory meta data
    self.metadata['array_layout'] = simtelMD['array_layout']
    self.metadata['site'] = simtelMD['site']
    self.metadata['particle'] = simtelMD['particle']
    self.metadata['phiP'] = simtelMD['phiP']
    self.metadata['thetaP'] = simtelMD['thetaP']
    self.metadata['analysis_prog'] = 'chimp'
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

    # step 2bis
    # arguments are nbFiles=0 (not used) and fileSize=100kB
    eivStep = self.setExecutable( '$DIRACROOT/scripts/cta-prod3-verifysteps', \
                              arguments = 'analysisinputs 0 100', \
                              logFile = 'Verify_AnalysisInputs_Log.txt' )
    eivStep['Value']['name'] = 'Step%i_VerifyAnalysisInputs' % iStep
    eivStep['Value']['descr_short'] = 'Verify Analysis Inputs'
    iStep += 1

    # step 3
    evStep = self.setExecutable( './dirac_prod3_chimp', \
                                arguments = '%s %s %s %s' % ( self.par1, self.par2, self.par3, self.par4 ),
                                logFile = 'Chimp_Log.txt' )
    evStep['Value']['name'] = 'Step%i_Chimp' % iStep
    evStep['Value']['descr_short'] = 'Run Chimp'
    iStep += 1

    # step 4
    # ## the order of the metadata dictionary is important, since it's used to build the directory structure
    mdjson = json.dumps( self.metadata )

    metadatafield = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)', 'particle':'VARCHAR(128)', \
                         'phiP':'float', 'thetaP': 'float', 'analysis_prog':'VARCHAR(128)', 'analysis_prog_version':'VARCHAR(128)'}

    mdfieldjson = json.dumps( metadatafield )

    fmdjson = json.dumps( self.filemetadata )

    dmStep = self.setExecutable( '$DIRACROOT/CTADIRAC/Core/scripts/cta-chimp-managedata.py',
                              arguments = "'%s' '%s' '%s' %s %s %s" % ( mdjson, mdfieldjson, fmdjson, self.basepath, self.outputpattern, self.package ),
                              logFile = 'DataManagement_Log.txt' )
    dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
    iStep += 1

"""
  Simple Wrapper on the Job class to handle User Prod3 MC
"""

__RCSID__ = "$Id$"

# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow import Workflow

class Prod3MCUserJob( Job ) :
  """ Job extension class for Prod3 MC simulations,
      takes care of running corsika
  """
        
  def __init__( self, cpuTime = 432000 ):
    """ Constructor
    
    Keyword arguments:
    cpuTime -- max cpu time allowed for the job
    """
    Job.__init__( self )
    self.setCPUTime( cpuTime )
    # defaults
    self.setName('Prod3MC_Generation')
    self.package='corsika_simhessarray'
    self.version = '2015-08-18'
    self.simtelopts = ''
    self.outputpattern = './*simtel.gz' 
    self.outputpath = '/vo.cta.in2p3.fr/user/a/arrabito'
    self.outputSE = 'DESY-ZN-USER'

  def setPackage(self, package):
    """ Set package name : e.g. 'corsika_simhessarray'
    
    Parameters:
    package -- corsika_simhessarray
    """
    self.package=package

  def setVersion(self, version):
    """ Set software version number : e.g. 2015-07-21
    
    Parameters:
    version -- corsika+simtel package version number
    """
    self.version=version

  def setInputCard( self, input_card ):
    """ Set corsika input card: e.g. INPUTS_CTA-trg-test-prod3-SST1_proton

    Parameters:
    input_card -- corsika input card
    """
    self.input_card = input_card

  def setSimtelCfg( self, simtelcfg ):
    """ Set sim_telarray configuration: e.g. ConfigPath/simtel.cfg

    Parameters:
    simtelcfg -- simtel cfg
    """
    self.simtelcfg = simtelcfg

  def setSimtelOpts( self, simtelopts ):
    """ Set sim_telarray options: e.g. TELESCOPE_THETA=20.0 TELESCOPE_PHI=90.0

    Parameters:
    simtelopts -- simtel opts
    """
    self.simtelopts = simtelopts

  def setupWorkflow( self, debug = False ):
    """ Setup job workflow by defining the sequence of all executables
        All parameters shall have been defined before that method is called.
    """

    self.workflow = Workflow()
    iStep=1
    
    ### execute corsika step
    self.executable = '$DIRACROOT/scripts/cta-prod3-corsika'
    argumentStr = '%s %s %s' % ( self.package, self.version, self.input_card )
    self.setConfigArgs( argumentStr )

    #### execute setup software: needed for next steps
    #swStep = self.setExecutable( '$DIRACROOT/scripts/cta-prod3-setupsw',
    #                          arguments='%s %s'% (self.package, self.version),\
    #                          logFile='SetupSoftware_Log.txt')
    #swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
    #swStep['Value']['descr_short'] = 'Setup software'
    #iStep+=1

    #### execute simtel_array step
    #simStep = self.setExecutable( './dirac_prod3_simtel_only_p1',
    #                          arguments='%s %s'% (self.simtelcfg, self.simtelopts),\
    #                          logFile='Simtel_Log.txt')
    #simStep['Value']['name'] = 'Step%i_Simtel' % iStep
    #simStep['Value']['descr_short'] = 'Run sim_telarray'
    #iStep+=1

    # step 2
    #res = DIRAC.sourceEnv(600, ['prod3_types'], {} )
    #read_cta_opts=res['outputEnv']['read_cta_opts']

    #rctaStep = self.setExecutable( './dirac_prod3_read_cta', \
    #                               arguments = "-q -r 4 -u --min-trg-tel 2 %s" % (read_cta_opts),
    #                            logFile = 'ReadCta_Log.txt' )
    #rctaStep['Value']['name'] = 'Step%i_ReadCta' % iStep
    #rctaStep['Value']['descr_short'] = 'Run ReadCta'
    #iStep += 1

    # step 4
    # ## put and register files (to be used in replacement of setOutputData of Job API)
    #dmStep = self.setExecutable( '$DIRACROOT/scripts/cta-user-managedata',
    #                          arguments = "%s %s %s" % ( self.outputpattern, self.outputpath, self.outputSE ),
    #                          logFile = 'DataManagement_Log.txt' )
    #dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
    #dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
    #iStep += 1


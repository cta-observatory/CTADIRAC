"""
  Simple Wrapper on the Job class to handle Prod3 MC
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow import Workflow
from DIRAC.Workflow.Utilities.Utils import getStepDefinition, addStepToWorkflow

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

  def setupWorkflow( self, debug = False ):
    """ Setup job workflow by defining the sequence of all executables
        All parameters shall have been defined before that method is called.
    """

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-prod3-corsika'
    argumentStr = '%s %s %s' % ( self.package, self.version, self.input_card )
    self.setConfigArgs( argumentStr )


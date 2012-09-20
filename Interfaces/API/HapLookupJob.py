"""
  Simple Wrapper on the Job class to handle HAP Lookup
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class HapApplicationJob( Job ) :


  def __init__( self, parameters = None, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-hap-lookup'  
    self.setCPUTime( cpuTime )
    argumentStr = "%s" % ( ' '.join( parameters ) )
    self.setConfigArgs( argumentStr )

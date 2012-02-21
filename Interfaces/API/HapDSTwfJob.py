"""
  Simple Wrapper on the Job class to handle HAP DST wf
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class HapDSTwfJob( Job ) :


  def __init__( self, script,  parameters = None, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-hap-wf'
    self.setName( os.path.basename( script ) )
    self.setCPUTime( cpuTime )
    global argumentStr
    argumentStr = "%s" % ( ' '.join( parameters ) )
    self.setConfigArgs( argumentStr )

  def setVersion(self, version):
    versionStr = ' ' + '-V' + ' ' + version
    global argumentStr
    argumentStr= argumentStr + versionStr
    self.setConfigArgs( argumentStr )
    
  def setConfig(self, config):
    configStr = ' ' + '-T' + ' ' + config
    global argumentStr
    argumentStr= argumentStr + configStr
    self.setConfigArgs( argumentStr )









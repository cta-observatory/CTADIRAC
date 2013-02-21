"""
  Simple Wrapper on the Job class to handle CorsikaSimtelUser
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class CorsikaSimtelUserJob( Job ) :


  def __init__( self, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-corsikasimteluser'
    self.setCPUTime( cpuTime )
    global argumentStr

  def setVersion(self, version):
    versionStr = '--version' + ' ' + version
    global argumentStr
    argumentStr= versionStr
    self.setConfigArgs( argumentStr )

  def setExecutable(self, executable):
    executableStr = ' ' + '-E' + ' ' + executable
    global argumentStr
    argumentStr= argumentStr + executableStr
    self.setConfigArgs( argumentStr )

  def setParameters(self, parameters = None):
    global argumentStr
    argumentStr = argumentStr + ' ' + "%s" % ( ' '.join( parameters ) )
    self.setConfigArgs( argumentStr )


   









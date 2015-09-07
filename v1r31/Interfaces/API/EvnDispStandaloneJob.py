"""
  Simple Wrapper on the Job class to handle EvnDisp
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class EvnDispStandaloneJob( Job ) :


  def __init__( self, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-evndispstandalone'
    self.setCPUTime( cpuTime )
    global parfileList
    parfileList = []
    global argumentStr

  def setVersion(self, version):
    versionStr = '-V' + ' ' + version
    global argumentStr
    argumentStr= versionStr
    self.setConfigArgs( argumentStr )

  def setExecutable(self, executable):
    executableStr = ' ' + '-E' + ' ' + executable
    global argumentStr
    argumentStr= argumentStr + executableStr
    self.setConfigArgs( argumentStr )

  def setEvnDispOpt(self, parameters = None):
    evndispparfile = 'evndisp.par'
    f = open( evndispparfile,'w')
    evndispargStr = "%s" % ( ' '.join( parameters ) )
    f.write(evndispargStr)
    f.close()
    global parfileList 
    parfileList.append(evndispparfile) 
    self.addToInputSandbox = ( parfileList )

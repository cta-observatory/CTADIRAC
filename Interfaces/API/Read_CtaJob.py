"""
  Simple Wrapper on the Job class to handle EvnDisp
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class Read_CtaJob( Job ) :


  def __init__( self, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    #self.executable = '$DIRACROOT/scripts/cta-read_cta'
    self.executable = '$DIRACROOT/CTADIRAC/Core/scripts/cta-read_cta.py'
    self.setCPUTime( cpuTime )
    global argumentStr

  def setVersion(self, version):
    versionStr = '-V' + ' ' + version
    global argumentStr
    argumentStr= versionStr
    self.setConfigArgs( argumentStr )
    
  def setRead_CtaOpt(self, parameters = None):
    readctaparfile = 'read_cta.par'
    f = open( readctaparfile,'w')
    readctaparargStr = "%s" % ( ' '.join( parameters ) )
    f.write(readctaparargStr)
    f.close()
    global parfileList 
    parfileList = [ readctaparfile] 
    self.addToInputSandbox = ( parfileList )

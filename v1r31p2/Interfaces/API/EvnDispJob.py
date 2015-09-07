"""
  Simple Wrapper on the Job class to handle EvnDisp
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class EvnDispJob( Job ) :


  def __init__( self, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-evndisp'
    self.setCPUTime( cpuTime )
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

  def setUseTrgFile(self, usetrgfile):
    usetrgfileStr = ' ' + '-T' + ' ' + usetrgfile
    global argumentStr
    argumentStr= argumentStr + usetrgfileStr
    self.setConfigArgs( argumentStr )
    
  def setLayoutList(self, layoutlist):
    layoutlistStr = ' ' + '-L' + ' ' + layoutlist
    global argumentStr
    argumentStr= argumentStr + layoutlistStr
    self.setConfigArgs( argumentStr )    

  def setMode(self, mode):
    modeStr = ' ' + '-M' + ' ' + mode
    global argumentStr
    argumentStr= argumentStr + modeStr
    self.setConfigArgs( argumentStr )  

  def setConverterOpt(self, parameters = None):
    converterparfile = 'converter.par'
    f = open( converterparfile,'w')
    converterparargStr = "%s" % ( ' '.join( parameters ) )
    f.write(converterparargStr)
    f.close()
    global parfileList 
    parfileList = [ converterparfile ] 
    self.addToInputSandbox = ( parfileList )

  def setEvnDispOpt(self, parameters = None):
    evndispparfile = 'evndisp.par'
    f = open( evndispparfile,'w')
    evndispargStr = "%s" % ( ' '.join( parameters ) )
    f.write(evndispargStr)
    f.close()
    global parfileList 
    parfileList.append(evndispparfile) 
    self.addToInputSandbox = ( parfileList )

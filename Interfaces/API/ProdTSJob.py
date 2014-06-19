"""
  Simple Wrapper on the Job class to handle Production Jobs
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class ProdTSJob( Job ) :


  def __init__( self, cpuTime = 3600 ):

    Job.__init__( self )
    self.setCPUTime( cpuTime )
    global argumentStr

  def setVersion(self, version):
    versionStr = '-V' + ' ' + version 
    global argumentStr
    argumentStr= versionStr

  def setApplication(self, application):
    executableStr = ' -E ' + application
    global argumentStr
    argumentStr= argumentStr + executableStr

  def setProdName(self, prodname):
    prodnameStr = ' -P ' + prodname
    global argumentStr
    argumentStr= argumentStr + prodnameStr

  def setPathRoot(self, pathroot):
    pathrootStr = ' -R ' + pathroot
    global argumentStr
    argumentStr= argumentStr + pathrootStr

  def setParameters(self, parameters = None):
    global argumentStr
    argumentStr = argumentStr + ' ' + "%s" % ( ' '.join( parameters ) )
    self.setExecutable('$DIRACROOT/scripts/cta-prodts',argumentStr)
















"""
  Simple Wrapper on the Job class to handle Reprocessing Production Jobs
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class ReproTSJob( Job ) :

  def __init__( self, cpuTime = 3600 ):

    Job.__init__( self )
    self.setCPUTime( cpuTime )
    global argumentStr

  def setVersion(self, version):
    versionStr = '-V' + ' ' + version 
    global argumentStr
    argumentStr= versionStr

  def setParameters(self, parameters = None):
    global argumentStr
    argumentStr = argumentStr + ' ' + "%s" % ( ' '.join( parameters ) )
    self.setExecutable('$DIRACROOT/scripts/cta-reprots',argumentStr)
















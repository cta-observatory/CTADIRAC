########################################################################
# File :   RootJob.py
# Author : R. Graciani
########################################################################
"""
  Simple Wrapper on the Job class to handle root Macros
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
#####################
from CTADIRAC.Core.Utilities import SoftwareInstallation
import os

class HapJob( Job ) :


  def __init__( self, script,  parameters = None, softwarePackage = 'HAP/v0.1/HAP',
                cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRACROOT/CTADIRAC/Core/scripts/cta-hap-application.py'
    self.setName( os.path.basename( script ) )
    self.setCPUTime( cpuTime )

    argumentStr = "%s" % ( ' '.join( parameters ) )

    self.setConfigArgs( argumentStr )

    
    self.__addSoftwarePackage( softwarePackage )


  def __addSoftwarePackage( self, package ):

    swPackages = 'SoftwarePackages'
    description = 'List of Software Packages to be installed'
    if not self.workflow.findParameter( swPackages ):
      self._addParameter( self.workflow, swPackages, 'JDL', package, description )
    else:
      apps = self.workflow.findParameter( swPackages ).getValue()
      if not package in string.split( apps, ';' ):
        apps += ';' + package
      self._addParameter( self.workflow, swPackages, 'JDL', apps, description )




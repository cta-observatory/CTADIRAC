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
import os

class RootJob( Job ) :


  def __init__( self, script, parameters = None, softwarePackage = 'HESS/v0.1/root',
               compiled = False, cpuTime = 3600 ):

    Job.__init__( self )

    self.workflow = Workflow()
    self.executable = '$DIRAC/scripts/cta-root-macro.py'
    self.setName( os.path.basename( script ) )
    self.setInputSandbox( [ script ] )
    self.setCPUTime( cpuTime )

    arguments = []
    toCompile = ''
    if compiled:
      toCompile = '+'

    if parameters:
      arguments = [ repr( k ).replace( '"', "\\\\'" ).replace( "'", "\\\\'" ) for k in parameters ]
    argumentStr = "%s%s %s" % ( os.path.basename( script ), toCompile, ' '.join( arguments ) )

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

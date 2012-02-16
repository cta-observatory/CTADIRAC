"""
  Simple Wrapper on the Job class to handle Hap root Macros
"""

__RCSID__ = "$Id$"

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Workflow                   import Workflow
import os

class HapDSTJob( Job ) :


  def __init__( self, script, parameters = None,
               compiled = False, cpuTime = 3600 ):

    Job.__init__( self )


    self.workflow = Workflow()
    self.executable = '$DIRACROOT/scripts/cta-hap-dst'
    self.setName( os.path.basename( script ) )
    self.setCPUTime( cpuTime )


    arguments = []
    toCompile = ''
    if compiled:
      toCompile = '+'

    if parameters:
      arguments = [ repr( k ).replace( '"', "\\\\'" ).replace( "'", "\\\\'" ) for k in parameters ]
    argumentStr = "%s%s %s" % ( os.path.basename( script ), toCompile, ' '.join( arguments ) )

    self.setConfigArgs( argumentStr )



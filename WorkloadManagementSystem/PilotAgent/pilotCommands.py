""" Definitions of a standard set of pilot commands

    Each commands is represented by a class inheriting CommandBase class.
    The command class constructor takes PilotParams object which is a data
    structure which keeps common parameters across all the pilot commands.

    The constructor must call the superclass constructor with the PilotParams
    object and the command name as arguments, e.g. ::

        class InstallDIRAC( CommandBase ):

          def __init__( self, pilotParams ):
            CommandBase.__init__(self, pilotParams, 'Install')
            ...

    The command class must implement execute() method for the actual command
    execution.
"""

import os

from pilotTools import CommandBase

__RCSID__ = '$Id$'

class ClearPythonPath( CommandBase ):
  """ Used to clean the PYTHONPATH
  """

  def execute( self ):
    """ Standard method for pilot commands
    """
    if 'PYTHONPATH' in os.environ:
      self.log.info( "PYTHONPATH found: %s" % os.environ['PYTHONPATH'] )
      self.log.info( "Clearing PYTHONPATH" )
      os.environ['PYTHONPATH']=""
      self.log.info( "PYTHONPATH set to empty: %s" % os.environ['PYTHONPATH'] )
    else:
      self.log.info( 'PYTHONPATH is not set' )   

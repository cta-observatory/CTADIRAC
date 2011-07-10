########################################################################
# File :   RootMacro.py
# Author : R. Graciani
########################################################################

""" The RootMacro class provides a simple way for CTA users to
  execute a Root Macro.
"""
__RCSID__ = "$Id$"

import DIRAC
import os

INPUTPARAMETERS = [ 'softwarePackage', 'rootMacro' ]

class RootMacro:
  """
    The Class containing the RootMacro Module code
    It requires the following Input Parameters to be defined:
     softwarePackage: Name of the CTA software package providing root.
     rootMacro: Name of the Macro to be executed.
     rootrootArguments List of rootArguments to be passed to the Macro (default: [] )
  """

  def __init__( self ):
    """
      Provide default values
    """
    self.log = DIRAC.gLogger.getSubLogger( "RootMacro" )
    self.rootArguments = []

  def __checkInputs( self ):
    """
      Check the data members for the expected Input Parameters
    """
    for inputPar in INPUTPARAMETERS:
      if not inputPar in dir( self ):
        error = 'Input Parameter not defined:'
        self.log.error( error, inputPar )
        return DIRAC.S_ERROR( ' '.join( [ error, inputPar ] ) )

    return DIRAC.S_OK()

  def execute( self ):
    """
      The method called by the Workflow framework
    """
    from DIRAC.Core.Utilities.Subprocess import systemCall
    from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
    ret = self.__checkInputs()
    if not ret['OK']:
      return ret

    ret = getSoftwareEnviron( self.softwarePackage )
    if not ret['OK']:
      error = ret['Message']
      self.log.error( error, self.softwarePackage )
      return DIRAC.S_ERROR( ' '.join( [ error, str( self.softwarePackage ) ] ) )

    rootEnviron = ret['Value']

    if not os.path.isfile( self.rootMacro ):
      error = 'Root macro file does not exist:'
      self.log.error( error, self.rootMacro )
      return DIRAC.S_ERROR( ' '.join( [ error, str( self.rootMacro ) ] ) )

    cmdTuple = ['root', '-b', '-q']
    cmdTuple += ['%s( %s )' % ( self.rootMacro, ', '.join( self.rootArguments ).replace( "'", '"' ) ) ]

    self.log.info( 'Executing command tuple:', cmdTuple )

    ret = systemCall( 0, cmdTuple, env = rootEnviron )

    if not ret['OK']:
      self.log.error( 'Failed to execute Root:', ret['Message'] )
      return DIRAC.S_ERROR( 'Can not execute root' )

    status, stdout, stderr = ret['Value']
    if status:
      self.log.error( 'Root execution reports Error:', status )
      self.log.error( stdout )
      self.log.error( stderr )
      return DIRAC.S_ERROR( 'Failed root Execution' )

    self.log.info( 'Root stdout:' )
    self.log.info( stdout )

    return DIRAC.S_OK()

  def setSoftwarePackage( self, softwarePackage = None ):
    """
      Define the software package or try to get it from the job.info file
      This method can be used when executing outside a Workflow
    """
    if softwarePackage:
      self.softwarePackage = softwarePackage
      return DIRAC.S_OK()
    try:
      infoFile = open( 'job.info' )
      for line in infoFile.readlines():
        if line.find( 'SoftwarePackages' ) > -1:
          self.softwarePackage = line.split( '=' )[-1].strip()
          break
      return DIRAC.S_OK()
    except Exception:
      return DIRAC.S_ERROR( 'Can not set softwarePackage' )

""" The HapDST class provides a simple way for CTA users to
  execute a make_CTA_DST.C Root Macro.
"""
__RCSID__ = "$Id$"

import DIRAC
import os

INPUTPARAMETERS = [ 'softwarePackage','rootMacro']

class HapDST:
  """
    The Class containing the HapDST Module code
    It requires the following Input Parameters to be defined:
     softwarePackage: Name of the CTA software package providing root.
     rootMacro: Name of the Macro to be executed.
     rootrootArguments List of rootArguments to be passed to the Macro (default: [] )
  """

  def __init__( self ):
    """
      Provide default values
    """
    self.log = DIRAC.gLogger.getSubLogger( "HapDST" )
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

  def sendOutput(self,stdid,line):
    DIRAC.gLogger.notice(line)

  def execute( self ):
    """
      The method called by the Workflow framework
    """
    from DIRAC.Core.Utilities.Subprocess import systemCall
    from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
    from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea

    ret = self.__checkInputs()
    if not ret['OK']:
      return ret

    ret = getSoftwareEnviron( self.softwarePackage )
    if not ret['OK']:
      error = ret['Message']
      self.log.error( error, self.softwarePackage )
      return DIRAC.S_ERROR( ' '.join( [ error, str( self.softwarePackage ) ] ) )

    hapEnviron = ret['Value']
    hessroot =  hapEnviron['HESSROOT']
    rootlogon_file = hessroot + '/rootlogon.C'  
    cp_cmd = 'cp ' + rootlogon_file + ' .'
    os.system(cp_cmd)
  
    fileName = hessroot + '/hapscripts/dst/' + self.rootMacro 

    if fileName[-1] == '+':
      # If the macro has to be compiled there is an extra "+" at the end of its name
      fileName = fileName[:-1]
 
    if not os.path.isfile( fileName ):
      error = 'make_CTA_DST.C file does not exist:'
      self.log.error( error, fileName )
      return DIRAC.S_ERROR( ' '.join( [ error, fileName ] ) )

    cmdTuple = ['root', '-b', '-q']

    configpath = hessroot + '/config/array' 
    self.rootArguments[2] = self.rootArguments[2].replace( "array", configpath ) 

    cmdTuple += ['%s( %s )' % ( self.rootMacro, ', '.join( self.rootArguments ).replace( "'", '"' ) ) ]

    self.log.notice( 'Executing command tuple:', cmdTuple )

    ret = systemCall( 0, cmdTuple, self.sendOutput, env = hapEnviron )

    if not ret['OK']:
      self.log.error( 'Failed to execute Root:', ret['Message'] )
      return DIRAC.S_ERROR( 'Can not execute root' )

    status, stdout, stderr = ret['Value']
    if status==0:
      self.log.error( 'make_CTA_DST.C execution reports Error:', status )
      self.log.error( stdout )
      self.log.error( stderr )
      return DIRAC.S_ERROR( 'Failed root Execution' )


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

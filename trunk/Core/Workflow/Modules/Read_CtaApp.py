""" The Read_CtaApp class provides a simple way for CTA users to
  execute a read_cta Application.
"""
__RCSID__ = "$Id$"

import DIRAC
import os

INPUTPARAMETERS = [ 'softwarePackage' ,'rctaExe']

class Read_CtaApp:
  """
    The Class containing the Read_ctaApp Module code
    It requires the following Input Parameters to be defined:
    softwarePackage: Name of the CTA software package providing read_cta.
    Executable:  Executable to be passed to the application
  """

  def __init__( self ):
    """
      Provide default values
    """
    self.log = DIRAC.gLogger.getSubLogger( "Read_CtaApp" )
    self.rctaArguments = []

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
    logfilename = self.rctaExe + '.log'
    f = open( logfilename,'a')
    f.write(line)
    f.write('\n')
    f.close()

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

    rctaEnviron = ret['Value']

    cmdTuple = [self.rctaExe]
    cmdTuple.extend(self.rctaArguments)  
 
    self.log.notice( 'Executing command tuple:', cmdTuple )

    ret = systemCall( 0, cmdTuple, self.sendOutput, env = rctaEnviron )

    if not ret['OK']:
      self.log.error( 'Failed to execute read_cta:', ret['Message'] )
      return DIRAC.S_ERROR( 'Can not execute read_cta' )

    status, stdout, stderr = ret['Value']

    self.log.notice( 'read_cta status is:', status )
    
    return status
    
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

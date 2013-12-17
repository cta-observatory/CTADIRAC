
""" The HapApplication class provides a simple way for CTA users to
  execute a Hap Application.
"""
__RCSID__ = "$Id$"

import DIRAC
import os

INPUTPARAMETERS = [ 'softwarePackage','hapExecutable' ]

class HapApplication:
  """
    The Class containing the HapConverter Module code
    It requires the following Input Parameters to be defined:
     softwarePackage: Name of the CTA software package providing hap.
     hapArguments: List of Arguments to be passed to the application (default: [] )
  """

  def __init__( self ):
    """
      Provide default values
    """
    self.log = DIRAC.gLogger.getSubLogger( "HapApplication" )
    self.hapArguments = []

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
    logfilename = self.hapExecutable + '.log'
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

    hapEnviron = ret['Value']

    cmdTuple = [self.hapExecutable]
    cmdTuple.extend(self.hapArguments)  
 
    self.log.notice( 'Executing command tuple:', cmdTuple )

    ret = systemCall( 0, cmdTuple, self.sendOutput, env = hapEnviron )

    if not ret['OK']:
      self.log.error( 'Failed to execute hap:', ret['Message'] )
      return DIRAC.S_ERROR( 'Can not execute hap' )

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

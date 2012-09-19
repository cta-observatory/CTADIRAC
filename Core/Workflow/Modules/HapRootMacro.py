""" The HapRootMacro class provides a simple way for CTA users to
  execute a HAP Root Macro.
"""
__RCSID__ = "$Id$"

import DIRAC
import os

INPUTPARAMETERS = [ 'softwarePackage','rootMacro']

class HapRootMacro:
  """
    The Class containing the HapRootMacr Module code
    It requires the following Input Parameters to be defined:
     softwarePackage: Name of the CTA software package providing root.
     rootMacro: Name of the Macro to be executed.
     rootrootArguments List of rootArguments to be passed to the Macro (default: [] )
  """

  def __init__( self ):
    """
      Provide default values
    """
    self.log = DIRAC.gLogger.getSubLogger( "HapRootMacro" )
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
    logfilename = self.rootMacro[:-2] + 'log'
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
      error = 'Hap Root macro file does not exist:'
      self.log.error( error, fileName )
      return DIRAC.S_ERROR( ' '.join( [ error, fileName ] ) )
     
    fileName = hessroot + '/hapscripts/dst/' + self.rootMacro   
      
    cmdTuple = ['root', '-b', '-q']
    
    cmdTuple += ['%s( %s )' % ( fileName, ', '.join( self.rootArguments ).replace( "'", '"' ) ) ]

    self.log.notice( 'Executing command tuple:', cmdTuple )

    ret = systemCall( 0, cmdTuple, self.sendOutput, env = hapEnviron )

    if not ret['OK']:
      self.log.error( 'Failed to execute Root:', ret['Message'] )
      return DIRAC.S_ERROR( 'Can not execute Hap root macro' )

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

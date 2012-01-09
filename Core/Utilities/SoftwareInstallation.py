#######################################################################
# $Id: SoftwareInstallation.py 384 2011-12-14 12:18:28Z arrabito@in2p3.fr $
# File :   SoftwareInstallation.py
# Author : Ricardo Graciani
########################################################################
"""
  This module handles:
   SoftwareInstallation.execute()
     check of SW installed in the shared software area, called by the JobAgent
     the Job parameter "SoftwarePackages" defines the list of packages to
     install.
     If any of the packages is not available in the shared software area, 
     it will attempt to do the installation in a LocalArea directory

  It also provides additional functions to handle the installation of SW and
  the access the installed SW

"""
__RCSID__ = "$Id"

from types import StringTypes, DictType
import os
import tarfile
from DIRAC.Core.Utilities import systemCall

SW_SHARED_DIR = 'VO_VO_CTA_IN2P3_FR_SW_DIR'
SW_DIR = 'software'
LFN_ROOT = '/vo.cta.in2p3.fr'
TIMEOUT = 600

import DIRAC
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

gLogger = DIRAC.gLogger.getSubLogger( __name__, True )

class SoftwareInstallation:
  """
    Module to check and install Software packages previous to job execution
  """

  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.job = {}
    if argumentsDict.has_key( 'Job' ):
      self.job = argumentsDict['Job']
    self.ce = {}
    if argumentsDict.has_key( 'CE' ):
      self.ce = argumentsDict['CE']
    self.source = {}
    if argumentsDict.has_key( 'Source' ):
      self.source = argumentsDict['Source']

    packs = []
    if 'SoftwarePackages' in self.job:
      if type( self.job['SoftwarePackages'] ) in StringTypes:
        packs = [self.job['SoftwarePackages']]
      else:
        packs = self.job['SoftwarePackages']

    self.packs = []
    for package in packs:
      gLogger.verbose( 'Requested Package %s' % package )
      self.packs.append( package )

    # If CTA has some naming convention for platforms it should be used here
    if self.job.has_key( 'SystemConfig' ):
      # If the job requested a certain platform we will use that
      self.jobConfig = self.job['SystemConfig']
    else:
      # Otherwise use the native platform
      self.jobConfig = DIRAC.platform

    self.ceConfigs = []
    if self.ce.has_key( 'CompatiblePlatforms' ):
      self.ceConfigs = self.ce['CompatiblePlatforms']
      if type( self.ceConfigs ) in StringTypes:
        self.ceConfigs = [self.ceConfigs]
    else:
      self.ceConfigs = [self.jobConfig]

    self.sharedArea = sharedArea()
    self.localArea = localArea()

  def execute( self ):
    """
      Check if requested packages are available in shared Area
      otherwise install them locally
    """
    # For the moment CTA does not use any kind if "platform" only slc5 64bit is supported ???
    if not self.packs:
      # There is nothing to do
      return DIRAC.S_OK()
    if self.sharedArea:
      gLogger.notice( 'Using Shared Area at:', self.sharedArea )
    if self.localArea:
      gLogger.notice( 'Using Local Area at:', self.localArea )

    for package in self.packs:
      gLogger.notice( 'Checking:', package )
      if self.sharedArea:
        if checkSoftwarePackage( package, self.sharedArea )['OK']:
          gLogger.notice( 'Package found in Shared Area:', package )
          continue
      if self.localArea:
        if checkSoftwarePackage( package, self.localArea )['OK']:
          gLogger.notice( 'Package found in Local Area:', package )
          continue
        if installSoftwarePackage( package, self.localArea )['OK']:
          continue
      gLogger.error( 'Check Failed for software package:', package )
      return DIRAC.S_ERROR( '%s not available' % package )

    return DIRAC.S_OK()


def installSoftwarePackage( package, area ):
  """
    Install the requested version of package under the given area
  """
  gLogger.notice( 'Installing software package:', ' at '.join( [package, area] ) ) 
  tarLFN = os.path.join( LFN_ROOT, SW_DIR, package ) + '.tar.gz'
  tarLFNcrypt = tarLFN + '.crypt'
  tarLFNs = [tarLFN,tarLFNcrypt]
  gLogger.notice( 'Trying to download a tarfile in the list:', tarLFNs )
  result = ReplicaManager().getFile( tarLFNs )
  
  if not result['OK']:
    gLogger.error( 'Failed to download tarfile:', tarLFNs )
    return result

  gLogger.notice( 'Check which tarfile is downloaded')

  for tar in tarLFNs:
    if tar in result['Value']['Successful']:
      gLogger.notice( 'Downloaded tarfile:', tar )
      packageTuple = package.split( '/' )
      tarFileName =  packageTuple[2] + '.tar.gz'       

      if tarLFNcrypt in result['Value']['Successful']:
      ##### decrypt #########################
        cryptedFileName = os.path.join( packageTuple[2] ) + '.tar.gz.crypt'
        cur_dir = os.getcwd()
        PassPhraseFile = cur_dir + '/passphrase'
        cmd = ['openssl','des3','-d','-in',cryptedFileName,
             '-out',tarFileName,'-pass','file:'+PassPhraseFile]
        DIRAC.gLogger.notice( 'decrypting:', ' '.join( cmd ) )
        ret = systemCall(0, cmd)
        status, stdout, stderr = ret['Value']
        DIRAC.gLogger.notice( 'decrypting reports status:', status )  
      try:
      ########## extract #######################
        tarMode = "r|*"
        installDir = os.path.join( area, packageTuple[0], packageTuple[1])  
        tar = tarfile.open( name = tarFileName, mode = tarMode )
        for tarInfo in tar:
          tar.extract( tarInfo, installDir )
        tar.close()
        os.unlink( tarFileName )
        gLogger.notice( 'Software package installed successfully:', package )
        return installSoftwareEnviron( package, area )

      except Exception:
        error = 'Failed to extract tarfile'
        gLogger.exception( '%s:' % error, tarFileName )
        return DIRAC.S_ERROR( error )
    else:
      gLogger.notice( 'Failed to download tarfile:', tar )

def _getEnvFileName( package, area ):
  """
    Produce Name of Environment File
  """
  packageTuple = package.split( '/' )
  return os.path.join( area,
                       packageTuple[0],
                       packageTuple[1],
                       '%sEnv.sh' % packageTuple[2].capitalize() )

def installSoftwareEnviron( package, area ):
  """
    Install Environment file for the given package
  """
  fileName = _getEnvFileName( package, area )
  try:
    if package == 'HESS/v0.1/lib':
      fd = open( fileName, 'w' )
      fd.write( """
export WORKING_DIR=%s/HESS/v0.1

export MYSQLPATH=${WORKING_DIR}/local
export MYSQL_LIBPATH=${WORKING_DIR}/local/lib/mysql
export MYSQL_PREFIX=${WORKING_DIR}/local

export PYTHONPATH=${WORKING_DIR}/local/lib/python2.6/site-packages
# unalias python
# alias python='python2.6'

export PATH=${WORKING_DIR}/local/bin:${PATH}
export LD_LIBRARY_PATH=${WORKING_DIR}/local/lib:${LD_LIBRARY_PATH}
""" % area )
      fd.close()
      return DIRAC.S_OK()
    if package == 'HESS/v0.1/root':
      fileName = os.path.join( area, 'HESS', 'v0.1', 'RootEnv.sh' )
      fd = open( fileName, 'w' )
      fd.write( """
export ROOTSYS=%s/HESS/v0.1/root

export PATH=${ROOTSYS}/bin:${PATH}
export LD_LIBRARY_PATH=${ROOTSYS}/lib:${LD_LIBRARY_PATH}
""" % area )
      return DIRAC.S_OK()
    if package == 'HAP/v0.1/HAP':

      if checkSoftwarePackage( 'HESS/v0.1/lib', sharedArea() )['OK']:
        gLogger.notice( 'Using LibEnv in Shared Area')
        libarea = sharedArea()
      else:
        gLogger.notice( 'Using LibEnv in Local Area')  
        libarea = localArea()
      if checkSoftwarePackage( 'HESS/v0.1/root', sharedArea() )['OK']:
        gLogger.notice( 'Using RootEnv in Shared Area')
        rootarea = sharedArea()
      else:
        gLogger.notice( 'Using RootEnv in Local Area')
        rootarea = localArea()

#      if os.environ.has_key( SW_SHARED_DIR ):
#        area = os.path.join( os.environ[SW_SHARED_DIR], SW_DIR )

      fd = open( fileName, 'w' )
      fd.write( """
export WORKING_DIR=%s/HESS/v0.1
export MYSQLPATH=${WORKING_DIR}/local
export MYSQL_LIBPATH=${WORKING_DIR}/local/lib/mysql
export MYSQL_PREFIX=${WORKING_DIR}/local
export PYTHONPATH=${WORKING_DIR}/local/lib/python2.6/site-packages
# unalias python
# alias python='python2.6'
export ROOTSYS=%s/HESS/v0.1/root
export HESSUSER=%s
export HESSROOT=${HESSUSER}/HAP/v0.1
export HESSVERSION=cta0311
export PARIS_MODULES=1
export PARIS_MODULES_MVA=0
export HAVE_FITS_MODULE=1
export CTA=1
export CTA_ULTRA=1
export NOPARIS=0
export PATH=${WORKING_DIR}/local/bin:${ROOTSYS}/bin:${HESSUSER}/bin:${HESSROOT}/bin:${PATH}
export LD_LIBRARY_PATH=${WORKING_DIR}/local/lib:${ROOTSYS}/lib:${HESSUSER}/lib:${HESSROOT}/lib:${HESSUSER}/lib/mysql:${LD_LIBRARY_PATH}
""" % (libarea, rootarea, localArea()))
      return DIRAC.S_OK()
  except Exception:
    gLogger.exception( 'Failed to install Environment file', package )
    return DIRAC.S_ERROR()
  return DIRAC.S_ERROR( 'Can not install Environment file for %s' % package )

def getSoftwareEnviron( package, environ = None ):
  """
    Check shared and local Area for the given package and return 
    the new environ dictionary
  """
  if environ == None:
    environ = dict( os.environ )
  elif type( environ ) == DictType:
    environ = dict( environ )
  else:
    return DIRAC.S_ERROR( 'environ argument must be a dictionary' )

  gLogger.notice( 'Getting environment for', package )

  for area in [sharedArea(), localArea()]: 
    if area:
      if not checkSoftwarePackage( package, area )['OK']:
        continue
      fileName = _getEnvFileName( package, area )
      base = os.path.splitext( fileName )[0]
      gLogger.info( 'Sourcing file to get environment:', fileName )
      result = DIRAC.sourceEnv( TIMEOUT, [base], environ )
      if not result['OK']:
        return result
      if result['stdout']:
        gLogger.info( result['stdout'] )
      return DIRAC.S_OK( result['outputEnv'] )

  # If we get here it means the package has not been found
  return DIRAC.S_ERROR( 'Failed to get Environment' )


def checkSoftwarePackage( package, area ):
  """
    Check if the requested version of package is installed under the given area
  """
  fileName = _getEnvFileName( package, area )
  gLogger.notice( 'Looking for Environment file', fileName )
  try:
    if os.path.exists( fileName ):
      return DIRAC.S_OK()
  except Exception:
    pass

  return DIRAC.S_ERROR()


def sharedArea():
  """
   Discover location of Shared SW area
   This area is populated by special jobs
  """
  area = ''
  if os.environ.has_key( SW_SHARED_DIR ):
    area = os.path.join( os.environ[SW_SHARED_DIR], SW_DIR )
    gLogger.debug( 'Using %s at "%s"' % ( SW_SHARED_DIR, area ) )
    if os.environ[SW_SHARED_DIR] == '.':
      if not os.path.isdir( SW_DIR ):
        os.mkdir( SW_DIR )
  elif DIRAC.gConfig.getValue( '/LocalSite/SharedArea', '' ):
    area = DIRAC.gConfig.getValue( '/LocalSite/SharedArea' )
    gLogger.debug( 'Using CE SharedArea at "%s"' % area )

  if area:
    # if defined, check that it really exists
    if not os.path.isdir( area ):
      gLogger.error( 'Missing Shared Area Directory:', area )
      print 'Missing Shared Area Directory:'
      area = ''

  return area

def createSharedArea():
  """
   Method to be used by special jobs to make sure the proper directory structure is created
   if it does not exists
  """
  if not os.environ.has_key( SW_SHARED_DIR ):
    gLogger.info( '%s not defined.' % SW_SHARED_DIR )
    return False

  area = os.environ[SW_SHARED_DIR]
  if area == '.':
    gLogger.info( '%s points to "."' % SW_SHARED_DIR )
    return False

  if not os.path.isdir( area ):
    gLogger.error( 'Shared area is not a directory:', '%s="%s"' % ( SW_SHARED_DIR, area ) )
    return False

  area = os.path.join( area, SW_DIR )
  try:
    if os.path.isdir( area ) and not os.path.islink( area ) :
      return True
    if not os.path.exists( area ):
      os.mkdir( area )
      return True
    os.remove( area )
    os.mkdir( area )
    return True
  except Exception, x:
    gLogger.error( 'Problem trying to create shared area', str( x ) )
    return False

def localArea():
  """
   Discover Location of Local SW Area.
   This area is populated by DIRAC job Agent for jobs needing SW not present
   in the Shared Area.
  """
  area = DIRAC.gConfig.getValue( '/LocalSite/LocalArea', '' )
  if not area:
    area = os.path.join( DIRAC.rootPath, SW_DIR )

  # check if already existing directory
  if not os.path.isdir( area ):
    # check if we can create it
    if os.path.exists( area ):
      try:
        os.remove( area )
      except Exception:
        gLogger.error( 'Cannot remove:', area )
        area = ''
    else:
      try:
        os.makedirs( area )
      except Exception:
        gLogger.error( 'Cannot create:', area )
        area = ''
  return area


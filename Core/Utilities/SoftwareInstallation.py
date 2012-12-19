#######################################################################
# $Id: SoftwareInstallation.py 416 2012-02-16 11:57:26Z arrabito@in2p3.fr $
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
        if area == workingArea():
          installDir = os.path.join( area ) 
        else:
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

  if area == workingArea():
    return os.path.join( area,
                        '%sEnv.sh' % packageTuple[2].capitalize() )
  else:
    return os.path.join( area,
                         packageTuple[0],
                         packageTuple[1],
                        '%sEnv.sh' % packageTuple[2].capitalize() )


def _getSoftwareDir( package, area ):
  """
    Produce Name of the Software Dir
  """
  packageTuple = package.split( '/' )
  return os.path.join( area,
                       packageTuple[0],
                       packageTuple[1])


def installSoftwareEnviron( package, area ):
  """
    Install Environment file for the given package
  """
  packageTuple = package.split( '/' )
  version = packageTuple[1]
  fileName = _getEnvFileName( package, area )
  
  try:
  
      if (packageTuple[0] == 'corsika_simhessarray' and version == 'test_18122012'):
        fd = open( fileName, 'w' )
        fd.write( """
unset HESSROOT
export HESSROOT
unset LD_LIBRARY_PATH
export LD_LIBRARY_PATH

# Top-level path under which we normally installed everything
if [ ! -z "${CTA_PATH}" ]; then
   cd "${CTA_PATH}" || exit 1
fi

./prepare_for_examples || exit 1

if [ ! -x corsika-run/corsika ]; then
   echo "No CORSIKA program found."
   exit 1
fi

if [ ! -x sim_telarray/bin/sim_telarray ]; then
   echo "No sim_telarray program found."
   exit 1
fi

# Top-level path under which we normally installed everything
if [ -z "${CTA_PATH}" ]; then
   export CTA_PATH="$(pwd -P)"
fi

# Paths to software, libraries, configuration files (read-only)
export CORSIKA_PATH="$(cd ${CTA_PATH}/corsika-run && pwd -P)"
export SIM_TELARRAY_PATH="$(cd ${CTA_PATH}/sim_telarray && pwd -P)"
export HESSIO_PATH="$(cd ${CTA_PATH}/hessioxxx && pwd -P)"
export LD_LIBRARY_PATH="${HESSIO_PATH}/lib"
export PATH="${HESSIO_PATH}/bin:${SIM_TELARRAY_PATH}/bin:${PATH}"

if [ -z "${MCDATA_PATH}" ]; then
   if [ ! -z "${CTA_DATA}" ]; then
      export MCDATA_PATH="${CTA_DATA}"
   else
      export MCDATA_PATH="${CTA_PATH}/Data"
   fi
fi

export CORSIKA_DATA="${MCDATA_PATH}/corsika"
export SIM_TELARRAY_DATA="${MCDATA_PATH}/sim_telarray"

printenv | egrep '^(CTA_PATH|CORSIKA_PATH|SIM_TELARRAY_PATH|SIM_TELARRAY_CONFIG_PATH|SIMTEL_CONFIG_PATH|HESSIO_PATH|CTA_DATA|MCDATA_PATH|CORSIKA_DATA|SIM_TELARRAY_DATA|HESSROOT|LD_LIBRARY_PATH|PATH|RUNPATH)=' | sort
""")
      fd.close()
      return DIRAC.S_OK()

    if packageTuple[0] == 'corsika_simhessarray':
      fd = open( fileName, 'w' )
      fd.write( """
unset HESSROOT
export HESSROOT
unset LD_LIBRARY_PATH
export LD_LIBRARY_PATH

./prepare_for_examples

if [ ! -x corsika-run/corsika ]; then
   echo "No CORSIKA program found."
   exit 1
fi

if [ ! -x sim_telarray/bin/sim_telarray ]; then
   echo "No sim_telarray program found."
   exit 1
fi

ln -s ./sim_telarray/cfg/common/atmprof1.dat

export CORSIKA_PATH="$(cd corsika-run && pwd -P)"
export SIM_TELARRAY_PATH="$(cd sim_telarray && pwd -P)"
export MCDATA_PATH="$(pwd -P)/Data"
export CORSIKA_DATA="${MCDATA_PATH}/corsika"
export SIM_TELARRAY_DATA="${MCDATA_PATH}/sim_telarray"
export CTA_PATH="$(pwd -P)"
export LD_LIBRARY_PATH="${CTA_PATH}/hessioxxx/lib"
export HESSIO_BIN="${PWD}/hessioxxx/bin"

export PATH="${PATH}:${HESSIO_BIN}:${SIM_TELARRAY_PATH}:${CORSIKA_PATH}"
""")
      fd.close()
      return DIRAC.S_OK()
    if package == 'PyFACT/v0.1/PyFACT':
      fd = open( fileName, 'w' )
      fd.write( """                                                                                                                                            
export EPDFREE=%s/epd_free/v0.1/epd_free                                                                                                                       
export PATH=${EPDFREE}/bin:${PATH}                                                                                                                             
export LD_LIBRARY_PATH=${EPDFREE}/lib:${LD_LIBRARY_PATH}                                                                                                       
export PYFACTROOT=%s/PyFACT/v0.1/PyFACT                                                                                                                        
""" % (area,area) )
      fd.close()
      return DIRAC.S_OK()      
    if package == 'epd_free/v0.1/epd_free':
      fd = open( fileName, 'w' )
      fd.write( """                                                                                                                                            
export EPDFREE=%s/epd_free/v0.1/epd_free                                                                                                                       
export PATH=${EPDFREE}/bin:${PATH}                                                                                                                             
export LD_LIBRARY_PATH=${EPDFREE}/lib:${LD_LIBRARY_PATH}                                                                                                       
""" % (area) )
      fd.close()
      return DIRAC.S_OK()
    if packageTuple[0] == 'ctools':
      fd = open( fileName, 'w' )
      fd.write( """
export GAMMALIB=%s/ctools/%s/ctools
source $GAMMALIB/bin/gammalib-init.sh
export CTOOLS=%s/ctools/%s/ctools
source $CTOOLS/bin/ctools-init.sh 
""" % (area,version,area,version) )
      fd.close()
      return DIRAC.S_OK()               

    if package == 'HESS/v0.2/lib':
      fd = open( fileName, 'w' )
      fd.write( """
export WORKING_DIR=%s/HESS/v0.2

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
    if package == 'HESS/v0.3/root':
      fileName = os.path.join( area, 'HESS', 'v0.3', 'RootEnv.sh' )
      fd = open( fileName, 'w' )
      fd.write( """
export ROOTSYS=%s/HESS/v0.3/root

export PATH=${ROOTSYS}/bin:${PATH}
export LD_LIBRARY_PATH=${ROOTSYS}/lib:${LD_LIBRARY_PATH}
""" % area )
      return DIRAC.S_OK()

    if packageTuple[0] == 'HAP':

      if checkSoftwarePackage( 'HESS/v0.2/lib', sharedArea() )['OK']:
        gLogger.notice( 'Using LibEnv in Shared Area')
        libarea = sharedArea()
      else:
        gLogger.notice( 'Using LibEnv in Local Area')  
        libarea = localArea()
      if checkSoftwarePackage( 'HESS/v0.3/root', sharedArea() )['OK']:
        gLogger.notice( 'Using RootEnv in Shared Area')
        rootarea = sharedArea()
      else:
        gLogger.notice( 'Using RootEnv in Local Area')
        rootarea = localArea()

      fd = open( fileName, 'w' )
      fd.write( """
export WORKING_DIR=%s/HESS/v0.2
export MYSQLPATH=${WORKING_DIR}/local
export PYTHONPATH=${WORKING_DIR}/local/lib/python2.6/site-packages
# unalias python
# alias python='python2.6'
export ROOTSYS=%s/HESS/v0.3/root
export HESSUSER=%s/HAP/%s
export HESSROOT=${HESSUSER}
export HESSCONFIG=${PWD}/IRF
#export HESSCONFIG=${HESSUSER}/IRF/
#export HESSVERSION=cta0312
export PARIS_MODULES=1
export PARIS_MODULES_MVA=0
export HAVE_FITS_MODULE=0
export CTA=1
export CTA_ULTRA=1
export NOPARIS=0
export MYSQL_LIBPATH=${HESSROOT}/lib

export LD_LIBRARY_PATH=/usr/lib64:${LD_LIBRARY_PATH}
export GCCPATH=/usr 
export CXX_KIND=/usr/bin/g++

export PATH=${GCCPATH}/bin:${ROOTSYS}/bin:${HESSUSER}/bin:${HESSROOT}/bin:${WORKING_DIR}/local/bin:${PATH}
export LD_LIBRARY_PATH=${WORKING_DIR}/local/lib:${GCCPATH}/lib:${ROOTSYS}/lib:${HESSUSER}/lib:${HESSROOT}/lib:${HESSROOT}/lib/mysql:${LD_LIBRARY_PATH}

""" % (libarea, rootarea, localArea(), version))
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

  for area in [workingArea(), sharedArea(), localArea()]: 
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
  softwareDir = _getSoftwareDir( package, area )
  try:
    gLogger.notice( 'Looking for Environment file', fileName )
    if os.path.exists( fileName ):
      return DIRAC.S_OK()
    else:
      gLogger.notice( 'Looking for Software dir', softwareDir )
      if os.path.isdir( softwareDir ):
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

def workingArea():
  """
   return working directory
  """
  area = os.environ['PWD']
  return area


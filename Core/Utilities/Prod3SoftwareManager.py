""" Module to setup Prod3 MC software
"""

__RCSID__ = "$Id$"

# generic imports
import os, tarfile

# DIRAC imports
import DIRAC
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

# @TODO
# handle different OS platform for the transition to CentOS7
# >>> import platform
# >>> platform.linux_distribution()
# ('CentOS Linux', '7.3.1611', 'Core')
# >>> platform.linux_distribution()
# ('Scientific Linux', '6.9', 'Carbon')

class Prod3SoftwareManager(object) :
  """ Manage software setup for prod3
  """

  def __init__( self, soft_category ):
    """ Constructor
    """
    self.SW_SHARED_DIR = 'VO_VO_CTA_IN2P3_FR_SW_DIR'
    self.CVMFS_DIR = '/cvmfs/cta.in2p3.fr/software'
    self.LFN_ROOT = '/vo.cta.in2p3.fr/software'
    self.SOFT_CATEGORY_DICT = soft_category
    self.dm = DataManager()

  def installDIRACScripts( self, package_dir ):
    """ copy prod3 DIRAC scripts in the current directory
    """
    cmd = 'cp ' + os.path.join( package_dir, 'dirac_*' ) + ' .'
    if not os.system( cmd ):
      return DIRAC.S_OK()
    else:
      return DIRAC.S_ERROR( 'Failed to install DIRAC scripts' )

  def dumpSetupScriptPath( self, package_dir, textfilename = 'setup_script_path.txt' ):
    """ dump the path to setupPackage.sh in a one line ascii file
          to be read and source by the following script
    """
    script_path = os.path.join( package_dir, 'setupPackage.sh' )
    open( textfilename, 'w' ).writelines( script_path + '\n' )
    return DIRAC.S_OK()

  def installSoftwarePackage( self, package, version, arch = "sl6-gcc44", installDir = '.' ):
    """ install software package in the current directory
    """
    DIRAC.gLogger.notice( 'Installing package %s version %s' % ( package, version ) )
    tarFile = package + '.tar.gz'
    tarLFN = os.path.join( self.LFN_ROOT, package, version, tarFile )

    ########## download the tar file #######################
    DIRAC.gLogger.notice( 'Trying to download package:', tarLFN )
    res = self.dm.getFile( tarLFN )
    if not res['OK']:
      return res

    if tarLFN in res['Value']['Successful']:
      DIRAC.gLogger.notice( ' Package downloaded successfully:', tarLFN )
    else:
      error = 'Failed to download package:', tarLFN
      return DIRAC.S_ERROR( error )

    ########## extract the tar file #######################
    tarMode = "r|*"
    tar = tarfile.open( tarFile, tarMode )
    for tarInfo in tar:
      tar.extract( tarInfo, installDir )
    tar.close()
    os.unlink( tarFile )

    DIRAC.gLogger.notice( 'Package %s version %s installed successfully at:\n%s' % ( package, version, installDir ) )

    return DIRAC.S_OK( installDir )

  def _getSoftwareAreas( self ):
    """ get the list of available software areas (shared area, cvmfs)
    """
    areaList = []

    opsHelper = Operations()
    UseCvmfs = opsHelper.getValue( 'SoftwarePolicy/UseCvmfs', bool )
    DIRAC.gLogger.notice( 'SoftwarePolicy for UseCvmfs is:', UseCvmfs )
    if UseCvmfs:
      areaList.append( self.CVMFS_DIR )

    if os.environ.has_key( self.SW_SHARED_DIR ):
      shared_area = os.path.join( os.environ[self.SW_SHARED_DIR], 'software' )
      areaList.append( shared_area )
    else:
      DIRAC.gLogger.warn( 'Shared area not found' )

    return areaList

  def _getSharedArea( self ):
    """ get Shared Area
    """
    if os.environ.has_key( self.SW_SHARED_DIR ):
      shared_area = os.path.join( os.environ[self.SW_SHARED_DIR], 'software' )
    else:
      return DIRAC.S_ERROR( 'Shared area not found' )

    return DIRAC.S_OK( shared_area )

  def _getPackageDir( self, area, arch, package, version ):
    """ get Package directory
    """
    package_dir = os.path.join( area, arch, self.SOFT_CATEGORY_DICT[package], package, version )
    return package_dir

  def removeSoftwarePackage( self, packagedir ):
    """ remove Software Package
    """
    cmd = 'rm -Rf ' + packagedir
    if not os.system( cmd ):
      return DIRAC.S_OK()
    else:
      error = 'Failed to remove %s' % packagedir
      return DIRAC.S_ERROR( error )


  def checkSoftwarePackage( self, package, version, arch = "sl6-gcc44", area = None ):
    """ check if the software package is installed in any software area
      Keyword arguments:
      package -- package name as the directory name
      version -- software version as the directory name
      arch -- architecture as the directory name
    """

    # if area is specified, just get the shared area
    if area:
      areaList = self._getSharedArea()
    else:
      areaList = self._getSoftwareAreas()

    if len( areaList ) == 0:
      DIRAC.gLogger.warn( 'No software area is available' )

    # ## look for the package directory in the software areas
    for area in areaList:
      package_dir = os.path.join( area, arch, self.SOFT_CATEGORY_DICT[package], package, version )
      if os.path.isdir( package_dir ):
        DIRAC.gLogger.notice( 'Found package %s version %s at:\n%s' % ( package, version, package_dir ) )
        return DIRAC.S_OK( package_dir )

    return DIRAC.S_ERROR( 'Could not find package %s version %s in any location' % ( package, version ) )

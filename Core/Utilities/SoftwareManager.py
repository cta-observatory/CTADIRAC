""" Module to find, install and setup software packages
        J. Bregeon, L. Arrabito
                15/09/2019
"""

__RCSID__ = "$Id$"

# generic imports
import os
import subprocess
import glob
import tarfile

# DIRAC imports
import DIRAC
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from CTADIRAC.Core.Utilities.tool_box import get_os_and_cpu_info


class SoftwareManager(object):
    """ Manage software setup
    """

    def __init__(self, soft_category):
        """ Constructor
        """
        self.CVMFS_DIR = '/cvmfs/cta.in2p3.fr/software'
        self.LFN_ROOT = '/vo.cta.in2p3.fr/software'
        self.SOFT_CATEGORY_DICT = soft_category
        self.dm = DataManager()

    def _search_software(self, package, version, compiler, use_cvmfs):
        ''' Look for sotfware package
        '''
        # software package category
        category = self.SOFT_CATEGORY_DICT[package]
        # look for software on cvmfs
        if use_cvmfs:
            package_dir = os.path.join(self.CVMFS_DIR, 'centos7',
                                       compiler, category, package, version)
            if os.path.isdir(package_dir):
                DIRAC.gLogger.notice('Found package %s version %s at:\n%s' %
                                     (package, version, package_dir))
                return DIRAC.S_OK({'Source':'cvmfs', 'Path':package_dir})
            else:
                DIRAC.gLogger.warn('%s\n not found on cvmfs'%package_dir)
        # look for tarball in the Dirac file catalog
        else:
            package_dir = os.path.join(self.LFN_ROOT, 'centos7',
                                       compiler, category, package, version)
            DIRAC.gLogger.notice('Looking for tarball in %s'%package_dir)
            results = self.dm.getFilesFromDirectory(package_dir)
            try:
                first_file_path = results['Value'][0]
                if first_file_path[-7:] == '.tar.gz':
                    results = self.dm.getActiveReplicas(first_file_path)
                    if results['OK']:
                        return DIRAC.S_OK({'Source':'tarball', 'Path':package_dir})
            except:
                DIRAC.gLogger.warn('No usual tarball found in the directory')

        return DIRAC.S_ERROR('Could not find package %s / %s / %s in any location'
                             % (package, version, compiler))

    def find_software(self, package, version, compiler='gcc48_default'):
        """ check if the software package is installed in any software area
          Keyword arguments:
          package -- package name as the directory name
          version -- software version as the directory name
          compiler -- compiler version and configuration
        """
        # first check if cvmfs is available
        ops_helper = Operations()
        use_cvmfs = ops_helper.getValue('SoftwarePolicy/UseCvmfs', bool)
        DIRAC.gLogger.notice('SoftwarePolicy for UseCvmfs is:', use_cvmfs)

        # get platform and cpu information
        try:
            os_name, cpu_name, inst = get_os_and_cpu_info()
            DIRAC.gLogger.notice('Running %s on a %s ' %(os_name, cpu_name))

        except:
            inst = 'sse4'
            DIRAC.gLogger.warn('Could not determine platform and cpu information')

        if compiler == 'gcc48_default':
            results = self._search_software(package, version, compiler, use_cvmfs)
            return results
        elif compiler == 'gcc48_sse4':
            # assume all processors have at least sse4
            results = self._search_software(package, version, compiler, use_cvmfs)
            return results
        elif compiler == 'gcc48_avx':
            if inst in ['avx', 'avx2', 'avx512']:
                results = self._search_software(package, version, compiler, use_cvmfs)
                return results
            else:
                DIRAC.gLogger.warn('CPU has no avx instructions, running sse4 version')
                compiler = 'gcc48_sse4'
                results = self._search_software(package, version, compiler, use_cvmfs)
                return results
        elif compiler == 'gcc48_avx2':
            if inst in ['avx2', 'avx512']:
                results = self._search_software(package, version, compiler, use_cvmfs)
                return results
            else:
                DIRAC.gLogger.warn('CPU has no avx2 instructions, running sse4 version')
                compiler = 'gcc48_sse4'
                results = self._search_software(package, version, compiler, use_cvmfs)
                return results
        elif compiler == 'gcc48_avx512':
            if inst is 'avx512':
                results = self._search_software(package, version, compiler, use_cvmfs)
                return results
            else:
                DIRAC.gLogger.warn('CPU has no avx512 instructions, running sse4 version')
                compiler = 'gcc48_sse4'
                results = self._search_software(package, version, compiler, use_cvmfs)
                return results
        elif compiler == 'gcc48_matchcpu':
            compiler = 'gcc48_%s'%inst
            results = self._search_software(package, version, compiler, use_cvmfs)
            return results
        else:
            DIRAC.S_ERROR('Unknown compiler specified: %s'%compiler)
        return DIRAC.S_ERROR('Could not find package %s version %s / %s in any location'
                             % (package, version, compiler))

    def install_dirac_scripts(self, package_dir):
        """ copy DIRAC scripts in the current directory
        """
        cmd = 'cp -f ' + os.path.join(package_dir, 'dirac_*') + ' .'
        try:
            subprocess.check_output(cmd, shell=True)
            return DIRAC.S_OK()
        except subprocess.CalledProcessError as error:
            return DIRAC.S_ERROR('Failed to install DIRAC scripts:\n%s'%error.output)

    def dump_setup_script_path(self, package_dir, textfilename = 'setup_script_path.txt'):
        """ dump the path to setupPackage.sh in a one line ascii file
              to be read and source by the following script
        """
        script_path = os.path.join(package_dir, 'setupPackage.sh')
        open(textfilename, 'w').writelines(script_path + '\n')
        return DIRAC.S_OK()

    def install_software(self, tar_lfn, target_dir='.'):
        """ install software package in the current directory
        """
        DIRAC.gLogger.notice('Installing package at %s'%tar_lfn)

        # Download the tar file
        DIRAC.gLogger.notice('Trying to download package:', tar_lfn)
        res = self.dm.getFile(tar_lfn)
        if not res['OK']:
            return res

        if tar_lfn in res['Value']['Successful']:
            DIRAC.gLogger.notice(' Package downloaded successfully:', tar_lfn)
        else:
            error = 'Failed to download package:', tar_lfn
            return DIRAC.S_ERROR(error)

        # Extract the tar file to the target directory
        tar_mode = "r|*"
        tar = tarfile.open(tar_lfn, tar_mode)
        for tarInfo in tar:
            tar.extract(tarInfo, target_dir)
        tar.close()
        os.unlink(tar_lfn)
        # Done
        DIRAC.gLogger.notice('Package %s installed successfully at:\n%s'
                             %(tar_lfn, target_dir))

        return DIRAC.S_OK(target_dir)

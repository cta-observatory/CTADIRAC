#!/usr/bin/env python
""" General script to setup software through the SoftwareManager module
        J. Bregeon, L. Arrabito 15/09/2019
"""

__RCSID__ = "$Id$"

# generic imports
import os, tarfile

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch("p:", "Package=", "Software package name")
Script.registerSwitch("v:", "Version=", "Base version to look for")
Script.registerSwitch("a:", "Category=", "Program category (simulations, analysis...)")
Script.registerSwitch("g:", "Compiler=", "Target a compiler_optimization configuration")
Script.registerSwitch("r:", "Repository=", "Target CVMFS repository")

Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s -p package -v version -a [program_category] -g [compiler]' % Script.scriptName,
                                     'Arguments:',
                                     '  package: corsika_simtelarray',
                                     '  version: 2019-09-03',
                                     '  program_category: simulations',
                                     '  compiler: gcc48_avx2',
                                     '\ne.g: %s -p corsika_simtelarray -v 2019-09-03'% Script.scriptName,
                                     ]))

Script.parseCommandLine(ignoreErrors=False)


# Specific DIRAC imports
from CTADIRAC.Core.Utilities.SoftwareManager import SoftwareManager

def setup_software(package, version, category, compiler, repository):
    """ setup a given software package to be used in the main workflow

    Keyword arguments:
        package: software name
        version: software version
        category: simulations, analysis...
        compiler: compiler and configuration
    """
    DIRAC.gLogger.notice('Trying to setup: %s %s %s %s'
                         %(package, version, category, compiler))
    # get arguments
    soft_category = {package:category}
    manager = SoftwareManager(soft_category)
    # for testing only
    if repository is not None:
        manager.CVMFS_DIR = repository
    # check if and where Package is available
    # return cvmfs/tarball and full path
    res = manager.find_software(package, version, compiler)
    if not res['OK']:  # could not find package
        return res
    source = res['Value']['Source']
    package_dir = res['Value']['Path']
    if source is 'cvmfs':
        res = manager.install_dirac_scripts(package_dir)
        if not res['OK']:
            return res
        res = manager.dump_setup_script_path(package_dir)
        if not res['OK']:
            return res
    elif source is 'tarball':
        res = manager.install_software(package_dir)
        if not res['OK']:
            return res
        package_local_path = res['Value']
        res = manager.dump_setup_script_path(package_local_path)
        if not res['OK']:
            return res
    return DIRAC.S_OK()


####################################################
if __name__ == '__main__':
    package = None
    version = None
    category = 'simulations'
    compiler = 'gcc48_default'
    repository = None
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "p" or switch[0].lower() == "package":
            package = switch[1]
        if switch[0] == "v" or switch[0].lower() == "version":
            version = switch[1]
        if switch[0] == "a" or switch[0].lower() == "category":
            category = switch[1]
        if switch[0] == "g" or switch[0].lower() == "compiler":
            compiler = switch[1]
        if switch[0] == "r" or switch[0].lower() == "repository":
            repository = switch[1]
    if package is None or version is None:
        DIRAC.gLogger.error('Please give a package name and a version')
        DIRAC.exit(-1)
    try:
        res = setup_software(package, version, category, compiler, repository)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Setup software completed successfully')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)

#!/usr/bin/env python
""" Data management script for production
    create DFC MetaData structure put and register files in DFC
    should work for corsika, simtel and EventDisplay output
    derived from cta-prod3-managedata.py and cta-analysis-managedata.py
                    JB September 2018
"""

__RCSID__ = "$Id$"

# generic imports
import os
import glob
import json

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine()

# Specific DIRAC imports
from CTADIRAC.Core.Workflow.Modules.Prod3DataManager import Prod3DataManager


def get_run_number(filename, package):
    """ try to get a run number from the file name
    """
    if filename[-9:] == '.logs.tgz':
        run_number = int(filename.split('/')[-1].split('_')[-1].split('.')[0])
    elif package in ['chimp', 'mars', 'corsika_simhessarray']:
        run_number = int(filename.split('run')[1].split('___cta')[0])
    elif package == 'evndisplay':
        if filename[-8:] in ['DL1.root', 'DL2.root']:
            run_number = int(filename.split('run')[1].split('___cta')[0])
        elif filename[-10:] in ['DL1.tar.gz', 'DL2.tar.gz']:
            run_number = int(filename.split('run')[1].split('___cta')[0])
        else:
            run_number = int(filename.split('-')[0])  # old default
    elif package == 'image_extractor':
        run_number = int(filename.split('srun')[1].split('-')[0])
    return str(run_number)


def put_and_register(args):
    """ simple wrapper to put and register all production files

    Keyword arguments:
    args -- a list of arguments in order []
    """
    metadata = args[0]
    metadata_fields = args[1]
    file_metadata = args[2]
    start_run_number = args[3]
    base_path = args[4]
    output_pattern = args[5]
    package = args[6]
    program_category = args[7]
    catalogs = args[8]
    output_type = args[9]

    # Load catalogs
    catalogs_json = json.loads(catalogs)

    # Create MD structure
    prod3dm = Prod3DataManager(catalogs_json)
    result = prod3dm.createMDStructure(metadata, metadata_fields, base_path, program_category)
    if result['OK']:
        path = result['Value']
    else:
        return result

    # Check the content of the output directory
    result = prod3dm._checkemptydir(output_pattern)
    if not result['OK']:
        return result

    # Loop over each file and upload and register
    for localfile in glob.glob(output_pattern):
        file_name = os.path.basename(localfile)
        # Check run number, assign one as file metadata if needed
        # or add start_run_number
        fmd_dict = json.loads(file_metadata)
        if not fmd_dict.has_key('runNumber'):
            run_number = get_run_number(file_name, package)
            fmd_dict['runNumber'] = '%08d' % run_number
        else:
            run_number = int(fmd_dict['runNumber'])+int(start_run_number)
            fmd_dict['runNumber'] = '%08d' % run_number

        # get the output file path
        run_path = prod3dm._getRunPath(fmd_dict)
        lfn = os.path.join(path, output_type, run_path, file_name)
        fmd_json = json.dumps(fmd_dict)
        result = prod3dm.putAndRegister(lfn, localfile, fmd_json, package)
        if not result['OK']:
            return result

    return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
    args = Script.getPositionalArgs()
    try:
        result = put_and_register(args)
        if not result['OK']:
            DIRAC.gLogger.error(result['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)

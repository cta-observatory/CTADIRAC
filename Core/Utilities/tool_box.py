""" Helper module that contains a set of useful simple and generic functions

                    JB, September 2018
"""

import os

import DIRAC
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

# Data level meta data id
DATA_LEVEL_METADATA_ID = {'MC0': -3,  'R1': -2, 'R0': -1,
                 'DL0': 0, 'DL1': 1, 'DL2': 2, 'DL3': 3, 'DL4': 4, 'DL5': 5}

def read_lfns_from_file(file_path):
    """ Read a simple list of LFNs from an ASCII files,
    expects just one LFN per line
    """
    content = open(file_path, 'r').readlines()
    input_file_list = []
    for line in content:
        infile = line.strip()
        if line != "\n":
            input_file_list.append(infile)
    return input_file_list

def run_number_from_filename(filename, package):
    """ try to get a run number from the file name

    return:
        run_number : int - the run number
    """
    if filename[-9:] == '.logs.tgz':
        run_number = int(filename.split('/')[-1].split('_')[-1].split('.')[0])
    elif package in ['chimp', 'mars']:
        run_number = int(filename.split('run')[1].split('___cta')[0])
    elif package in ['corsika_simhessarray']:
        if filename[-12:] in ['.corsika.zst']:
	    # int(re.findall(r'run\d+_', os.path.basename(filename))[0].strip('run_'))
            run_number = int(os.path.basename(filename).split('_')[0].strip('run'))
        elif os.path.splitext(filename)[1] in ['.log']:
            run_number = int(os.path.basename(filename).strip('run.log'))
        else:
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
    return run_number

def check_dataset_query(dataset_name):
    """ print dfind command for a given dataset
    """
    md_dict = get_dataset_MQ(dataset_name)
    return debug_query(md_dict)

def debug_query(MDdict):
    """ just unwrap a meta data dictionnary into a dfind command
    """
    msg='dfind /vo.cta.in2p3.fr/MC/'
    for key,val in MDdict.items():
        try:
            val = val.values()[0]
        except:
            pass
        msg+=' %s=%s' % (key, val)
    return msg

def get_dataset_MQ(dataset_name):
    """ Return the Meta Query associated with a given dataset
    """
    fc = FileCatalogClient()
    result = returnSingleResult(fc.getDatasetParameters(dataset_name))
    if not result['OK']:
        DIRAC.gLogger.error("Failed to retrieved dataset:",
                            result['Message'])
        DIRAC.exit(-1)
    else:
        DIRAC.gLogger.info("Successfully retrieved dataset: ", dataset_name)
    return result['Value']['MetaQuery']

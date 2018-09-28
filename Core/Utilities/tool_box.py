""" Helper module that contains a set of useful simple and generic functions

                    JB, September 2018
"""

import os


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
            run_number = int(os.path.basename(filename).split('_')[0].strip('run'))
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

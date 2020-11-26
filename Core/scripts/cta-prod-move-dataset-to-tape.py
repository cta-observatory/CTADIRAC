#!/usr/bin/env python
'''
Move a dataset from a disk storage to a tape storage

    J. Bregeon, November 2020
    bregeon@in2p3.fr
'''

__RCSID__ = "$Id$"


import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Utilities.ReplicationTransformation import createDataTransformation


def get_dataset_info(dataset_name):
    """ Return essential dataset information
        Name, number of files, total size and meta query
    """
    fc = FileCatalogClient()
    result = fc.getDatasets(dataset_name)
    if not result['OK']:
        print("ERROR: failed to get datasets")
        print(result)
        DIRAC.exit(-1)
    dataset_dict = result['Value']
    res = dataset_dict['Successful'][dataset_name][dataset_name]
    number_of_files = res['NumberOfFiles']
    meta_query = res['MetaQuery']
    total_size = res['TotalSize']
    return (dataset_name, number_of_files, total_size, meta_query)


#########################################################
if __name__ == '__main__':
    """
    """
    Script.parseCommandLine(ignoreErrors=True)
    argss = Script.getPositionalArgs()
    dataset_name = argss[0]
    tape_se = argss[1]

    tc = TransformationClient()

    # Check input data set information
    name, n_files, size, meta_query = get_dataset_info(dataset_name)
    print('Found dataset %s with %d files.' % (name, n_files))

    # choose a metaKey
    meta_key = 'analysis_prog'
    meta_value = 'merge_simtel'
    tag = ''
    do_it = True
    se_list = ['CC-IN2P3-Disk', 'DESY-ZN-Disk', 'CYF-STORM-Disk']

    # create Transformation
    data_ts = createDataTransformation(flavour='Moving',
                                       targetSE=tape_se,
                                       sourceSE=se_list,
                                       metaKey=meta_key,
                                       metaValue=meta_value,
                                       extraData=meta_query,
                                       extraname=tag,
                                       groupSize=1,
                                       plugin='Broadcast',
                                       tGroup=None,
                                       tBody=None,
                                       enable=do_it,
                                       )

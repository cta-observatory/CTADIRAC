#!/usr/bin/env python

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get statistics for datasets corresponding to the dataset name (may contain wild card)
if no dataset is specified it gives the list of available datasets
Usage:
   %s <dataset>

""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Utilities.PrettyPrint import printTable


def print_dataset_list(dataset_list):
    dataset_list.sort()
    print('\nAvailable datasets are:\n')
    for dataset_name in dataset_list:
        print(dataset_name)
    print '\n'

def get_list_of_datasets(tag=''):
    fc = FileCatalogClient()
    dataset_tag = '*%s*' % tag
    result = fc.getDatasets(dataset_tag)
    if not result['OK']:
        print("ERROR: failed to get datasets")
        DIRAC.exit(-1)
    dataset_dict = result['Value']
    dataset_list = list()
    for dataset_name in dataset_dict['Successful'][dataset_tag].keys():
        dataset_list.append(dataset_name)
    return dataset_list

def get_dataset_info(dataset_name):
    fc = FileCatalogClient()
    result = fc.getDatasets(dataset_name)
    if not result['OK']:
        print("ERROR: failed to get datasets")
        DIRAC.exit(-1)
    dataset_dict = result['Value']
    res = dataset_dict['Successful'][dataset_name][dataset_name]
    number_of_files = res['NumberOfFiles']
    meta_query = res['MetaQuery']
    total_size = res['TotalSize']
    return (dataset_name, number_of_files, total_size,meta_query)

# Main
argss = Script.getPositionalArgs()

if len(argss) == 0:
    dataset_list = get_list_of_datasets()
    print_dataset_list(dataset_list)
    DIRAC.exit()
elif len(argss) > 0:
    dataset_name = argss[0]
    dataset_list = list()
    if dataset_name.find('*')>0:
        dataset_list = get_list_of_datasets(dataset_name)
    else:
        dataset_list.append(dataset_name)
    print_dataset_list(dataset_list)

# Results
print('Datasets details')
print('| Name | N files | Size(TB) |')
dataset_list.sort()
for dataset_name in dataset_list:
    name, n_files, size, mq = get_dataset_info(dataset_name)
    # # convert total size in TB
    size_TB = size / 1e12
    print('|%s|%d|%.1f|' % (name, n_files, size_TB))

# print '\n' + datasetName + ":"
# print '=' * ( len( datasetName ) + 1 )
#
#
# # Fill the table to display
# records.append( ['MetaQuery', str( metaQuery )] )
#
# # # calculate total numberOfEvents
# TotalNumberOfEvents = numberOfFiles * int( eventsPerRun ) / float(numberOfFilesperRun) / 1e9
# TotalNumberOfEvents = '%.2fe9' % TotalNumberOfEvents
#
# records.append( ['EventsPerRun', str( eventsPerRun )] )
# records.append( ['TotalNumberOfEvents', str( TotalNumberOfEvents )] )
# records.append( ['NumberOfFiles', str( numberOfFiles )] )
#

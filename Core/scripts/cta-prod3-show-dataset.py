#!/usr/bin/env python

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get PROD3 statistics for a given dataset
if no dataset is specified it gives the list of available datasets
Usage:
   %s <dataset>

""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Utilities.PrettyPrint import printTable

argss = Script.getPositionalArgs()

fc = FileCatalogClient()

datasetName = ''
if len( argss ) > 0:
  datasetName = argss[0]

result = fc.getDatasets( datasetName )
if not result['OK']:
  print "ERROR: failed to get datasets"
  DIRAC.exit( -1 )

datasetDict = result['Value']

if len( argss ) == 0:
  print '\nAvailable datasets are:\n'
  for dName in datasetDict.keys():
    print dName
  DIRAC.exit()

fields = ['Key', 'Value']
datasets = datasetDict.keys()

for dName in datasets:
  records = []
  print '\n' + dName + ":"
  print '=' * ( len( dName ) + 1 )

  records.append( ['MetaQuery', str( datasetDict[dName]['MetaQuery'] )] )
  records.append( ['NumberOfFiles', str( datasetDict[dName]['NumberOfFiles'] )] )
  # # convert total size in TB
  totalsize = datasetDict[dName]['TotalSize'] / 1e12
  totalsize = '%.1f TB' % totalsize
  records.append( ['TotalSize', totalsize] )

  printTable( fields, records )

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
  print '\n'
  DIRAC.exit()

fields = ['Key', 'Value']

eventsPerRun = raw_input( '\nEnter eventsPerRun (default 20000):' )
# ## default
if not eventsPerRun:
  eventsPerRun = 20000

numberOfFilesperRun = raw_input( '\nEnter numberOfFilesperRun (10 with sct, 5 without sct)\ndefault is 10:' )
# ## default
if not numberOfFilesperRun:
  numberOfFilesperRun = 10

records = []
print '\n' + datasetName + ":"
print '=' * ( len( datasetName ) + 1 )

numberOfFiles = datasetDict[datasetName]['NumberOfFiles']

records.append( ['MetaQuery', str( datasetDict[datasetName]['MetaQuery'] )] )

# # calculate total numberOfEvents
TotalNumberOfEvents = numberOfFiles * int( eventsPerRun ) / float(numberOfFilesperRun) / 1e9
TotalNumberOfEvents = '%.2fe9' % TotalNumberOfEvents

records.append( ['EventsPerRun', str( eventsPerRun )] )
records.append( ['TotalNumberOfEvents', str( TotalNumberOfEvents )] )

records.append( ['NumberOfFiles', str( datasetDict[datasetName]['NumberOfFiles'] )] )

  # # convert total size in TB
totalsize = datasetDict[datasetName]['TotalSize'] / 1e12
totalsize = '%.1f TB' % totalsize
records.append( ['TotalSize', totalsize] )


printTable( fields, records )

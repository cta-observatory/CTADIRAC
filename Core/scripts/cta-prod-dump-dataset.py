#!/usr/bin/env python

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger

Script.setUsageMessage( """
Dump in a file the list of PROD3 files for a given dataset

Usage:
   %s <dataset>
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

argss = Script.getPositionalArgs()

fc = FileCatalogClient()

if len( argss ) > 0:
  datasetName = argss[0]
else:
  Script.showHelp()

result = fc.getDatasetFiles( datasetName )

if not result['OK']:
  print "ERROR: failed to get files for dataset:", result['Message']
else:
  lfnList = result['Value']['Successful'][datasetName]

  f = open ( datasetName + '.list', 'w' )
  for lfn in lfnList:
    f.write( lfn + '\n' )
  f.close()
  gLogger.notice( '%d files have been put in %s.list' % ( len( lfnList ), datasetName ) )


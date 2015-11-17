#!/usr/bin/env python

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

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
  lfnList = result['Value']

  f = open ( datasetName + '.list', 'w' )
  for lfn in lfnList:
    f.write( lfn + '\n' )
  f.close()

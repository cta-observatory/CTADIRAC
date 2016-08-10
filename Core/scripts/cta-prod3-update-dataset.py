#!/usr/bin/env python

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Update a given dataset

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

result = fc.updateDataset( datasetName )
if not result['OK']:
  print "ERROR: failed to update dataset %s: %s" % ( datasetName, result['Message'] )
else:
  print "Successfully updated dataset", datasetName
  

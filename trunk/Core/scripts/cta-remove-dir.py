#!/usr/bin/env python
"""
Remove empty directories
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

args = Script.getPositionalArgs()
infile = args[0]

fcc = FileCatalogClient()

f = open (infile, 'r')
for line in f:
  res = fcc.removeDirectory(line.strip(),True)
  print res
      



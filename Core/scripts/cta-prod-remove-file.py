#!/usr/bin/env python

__RCSID__ = "$Id$"

# generic imports
from multiprocessing import Pool

# DIRAC imports
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Bulk removal of a list of files 
Usage:
   %s <ascii file with lfn list>

""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
fc = FileCatalog()

def removeFile(lfn):
    fc.removeFile(lfn)
    if not res['OK']:
      print 'Error removing lfn: ' + lfn
      return res['Message']
  
if __name__ == '__main__':

    args = Script.getPositionalArgs()
    if len( args ) > 0:
      infile = args[0]
    else:
      Script.showHelp()

    f = open( infile, 'r' )
    infileList = []
    for line in f:
      infile = line.strip()
      if line != "\n":
        infileList.append( infile )

    p = Pool(10)
    p.map(removeFile, infileList)

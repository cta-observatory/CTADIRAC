#!/usr/bin/env python

__RCSID__ = "$Id$"

# generic imports
from multiprocessing import Pool

# DIRAC imports
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Bulk removal of a list of files from the catalog
Usage:
   %s <ascii file with lfn list>

""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
fc = FileCatalog()

def unregisterFile(lfn):
  res = fc.removeFile(lfn)
  if res['OK']:
    if 'Failed' in res['Value']:
      if lfn in res['Value']['Failed']:
        print "ERROR: %s" % ( res['Value']['Failed'][lfn] )
      elif lfn in res['Value']['Successful']:
        print "File",lfn,"removed from the catalog"
      else:
        print "ERROR: Unexpected result %s" % res['Value']
    else:
      print "File",lfn,"removed from the catalog"
  else:
    print "Failed to remove file from the catalog"  
    print res['Message']
      
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
    p.map(unregisterFile, infileList)

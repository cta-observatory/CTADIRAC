#!/usr/bin/env python

__RCSID__ = "$Id$"

# generic imports
from multiprocessing import Pool

# DIRAC imports
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Bulk retrieval of a list of files from Grid storage to the current directory
Usage:
   %s <ascii file with lfn list>

""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac import Dirac

def getfile(lfn):

  dirac = Dirac()
  res = dirac.getFile( lfn )
  if not res['OK']:
    print 'Error downloading lfn: ' + lfn
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
    p.map(getfile, infileList)

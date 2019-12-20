#!/usr/bin/env python

__RCSID__ = "$Id$"

# generic imports
import os
from multiprocessing import Pool

# DIRAC imports
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Bulk upload of a list of local files from the current directory to a Storage Element
Usage:
   %s <ascii file with lfn list> <SE>
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

if len( args ) > 1:
  infile = args[0]
  SE = args[1]
else:
  Script.showHelp()

from DIRAC.Interfaces.API.Dirac import Dirac

def addfile(lfn):

  dirac = Dirac()
  res = dirac.addFile(lfn,os.path.basename(lfn),SE)
  if not res['OK']:
    print 'Error uploading lfn: ' + lfn
    return res['Message']

if __name__ == '__main__':

    f = open( infile, 'r' )
    infileList = []
    for line in f:
      infile = line.strip()
      if line != "\n":
        infileList.append( infile )

    p = Pool(10)
    p.map(addfile, infileList)

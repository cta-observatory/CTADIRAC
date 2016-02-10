#!/usr/bin/env python
""" Simple data management script for users
    put and register files in DFC
"""

__RCSID__ = "$Id$"

# generic imports
import os, glob

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine()

# Specific DIRAC imports
from CTADIRAC.Core.Workflow.Modules.Prod3DataManager import Prod3DataManager
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

####################################################
def putAndRegisterPROD3( args ):
    """ simple wrapper to put and register all analysis files

    Keyword arguments:
    args -- a list of arguments in order []
    """
    outputpattern = args[0]
    outputpath = args[1]
    SE = args[2]

    catalogs = ['DIRACFileCatalog']
    dm = DataManager( catalogs )

    # # Init DataManager
    prod3dm = Prod3DataManager( catalogs )

    # # Upload data files
    res = prod3dm._checkemptydir( outputpattern )
    if not res['OK']:
      return res

    for localfile in glob.glob( outputpattern ):
      filename = os.path.basename( localfile )
      lfn = os.path.join( outputpath, filename )
      msg = 'Try to upload local file: %s \nwith LFN: %s \nto %s' % ( localfile, lfn, SE )
      DIRAC.gLogger.notice( msg )
      res = dm.putAndRegister( lfn, localfile, SE )
      # ##  check if failed
      if not res['OK']:
        DIRAC.gLogger.error( 'Failed to putAndRegister %s \nto %s \nwith message: %s' % ( lfn, SE, res['Message'] ) )
        return res
      elif res['Value']['Failed'].has_key( lfn ):
        DIRAC.gLogger.error( 'Failed to putAndRegister %s to %s' % ( lfn, SE ) )
        return res

    return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
  args = Script.getPositionalArgs()
  try:
    res = putAndRegisterPROD3( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

#!/usr/bin/env python
""" Simple data management script for PROD3 MC
    create DFC MetaData structure
    put and register files in DFC
"""

__RCSID__ = "$Id$"

# generic imports
import os, glob, json

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script
 
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s one two' % Script.scriptName,
                                     'Arguments:',
                                     '  one: one',
                                     '\ne.g: %s ?' % Script.scriptName
                                     ] ) )

Script.parseCommandLine()

# Specific DIRAC imports
from CTADIRAC.Core.Workflow.Modules.Prod3DataManager import Prod3DataManager

####################################################
def putAndRegisterEvnDisp( args ):
    """ simple wrapper to put and register all PROD3 files
    
    Keyword arguments:
    args -- a list of arguments in order []
    """
    metadata = args[0] 
    metadatafield = args[1]
    filemetadata = args[2]
    inputpath = args[3]
    basepath = args[4]
    catalogs = ['DIRACFileCatalog']
    
    # # Create MD structure
    prod3dm=Prod3DataManager(catalogs)
    res = prod3dm.createMDStructure( metadata, metadatafield, basepath )
    if res['OK']:
      path = res['Value']
    else:
      return res

    # # Upload data files
    datadir = os.path.join( inputpath, '*.root' )
    res = prod3dm._checkemptydir( datadir )
    if not res['OK']:
      return res

    # ## Get the run number
    fmd = json.loads( filemetadata )
    fmdjson = json.dumps( fmd )

    # ## Get the run directory
    runpath = prod3dm._getRunPath( fmdjson )

    for localfile in glob.glob( datadir ):
      filename = os.path.basename( localfile )
      lfn = os.path.join( path, 'Data', runpath, filename )
      res = prod3dm.putAndRegister( lfn, localfile, fmdjson, 'EvnDisp' )
      if not res['OK']:
        return res

    # ## Upload log files????

    return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
  
  DIRAC.gLogger.setLevel('VERBOSE')
  args = Script.getPositionalArgs()
  try:    
    res = putAndRegisterEvnDisp( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

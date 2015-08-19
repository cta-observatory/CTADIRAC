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
def putAndRegisterPROD3(args):
    """ simple wrapper to put and register all PROD3 files
    
    Keyword arguments:
    args -- a list of arguments in order []
    """
    metadata = args[0] 
    metadatafield = args[1]
    filemetadata = args[2]
    inputpath = args[3]
    basepath = args[4]
    start_run_nb = args[5]
    catalogs = ['DIRACFileCatalog']
    
    # # Create MD structure
    prod3dm=Prod3DataManager(catalogs)
    res = prod3dm.createMDStructure( metadata, metadatafield, basepath )
    if res['OK']:
      path = res['Value']
    else:
      return res

    # # Upload data files
    datadir = os.path.join( inputpath, 'Data/*' )
    res = prod3dm._checkemptydir( datadir )
    if not res['OK']:
      return res

    # ## Add start_run_number to run_number and update filemetadata
    fmd = json.loads( filemetadata )
    run_number = int( fmd['runNumber'] ) + int( start_run_nb )
    fmd['runNumber'] = '%08d' % run_number
    fmdjson = json.dumps( fmd )

    # ## Get the runp directory
    runpath = prod3dm._getRunPath( fmdjson )

    for localfile in glob.glob( datadir ):
      filename = os.path.basename( localfile )
      lfn = os.path.join( path, 'Data', runpath, filename )
      res = prod3dm.putAndRegister( lfn, localfile, fmdjson )
      if not res['OK']:
        return res

    # ## Upload log files
    tarname = filename.split( '-subarray' )[0] + '.tar.gz'
    res = prod3dm.createTarLogFiles( inputpath, tarname )
    if not res['OK']:
      return DIRAC.S_ERROR( 'prod3dm.createTarLogFiles failed' )
    lfn = os.path.join( path, 'Log', runpath, tarname )
    # res = prod3dm.putAndRegister( lfn, tarname, filemetadata )
    res = prod3dm.putAndRegister( lfn, tarname, fmdjson )
    if not res['OK']:
      return res

    return DIRAC.S_OK()

####################################################
if __name__ == '__main__':
  
  DIRAC.gLogger.setLevel('VERBOSE')
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

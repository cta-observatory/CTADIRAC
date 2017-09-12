#!/usr/bin/env python
""" Simple data management script for Analysis PROD3 MC
    create DFC MetaData structure
    put and register files in DFC
"""

__RCSID__ = "$Id$"

# generic imports
import os
import glob
import json

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine()

# Specific DIRAC imports
from CTADIRAC.Core.Workflow.Modules.Prod3DataManager import Prod3DataManager

def getRunNumber( filename, package ):
    if filename[-9:] == '.logs.tgz':
        run_number = int(filename.split('/')[-1].split('_')[-1].split('.')[0])       
    elif package in ['chimp', 'mars', 'corsika_simhessarray']:
        run_number = int(filename.split( 'run' )[1].split( '___cta' )[0])
    elif package == 'evndisplay':
        if filename[-8:] == 'DL1.root':
            run_number = int(filename.split( 'run' )[1].split( '___cta' )[0])
        elif filename[-8:] == 'DL2.root':
            run_number = int(filename.split( 'tid' )[1].split( '___cta' )[0])
        else:         
            run_number = int(filename.split( '-' )[0]) # old default
    return str(run_number)

####################################################
def putAndRegisterPROD3( args ):
    """ simple wrapper to put and register all analysis files

    Keyword arguments:
    args -- a list of arguments in order []
    """
    metadata = args[0]
    metadatafield = args[1]
    filemetadata = args[2]
    basepath = args[3]
    outputpattern = args[4]
    package = args[5]
    program_category = args[6]
    catalogs = args[7]
    catalogsjson = json.loads( catalogs )
    if len(args)==8:
      outputType='Data'
    else:
      outputType='Log'

    # # Create MD structure
    prod3dm = Prod3DataManager( catalogsjson )
    #res = prod3dm.createMDStructure( metadata, metadatafield, basepath, 'analysis')
    res = prod3dm.createMDStructure( metadata, metadatafield, basepath, program_category)
    if res['OK']:
      path = res['Value']
    else:
      return res

    # # Upload data files
    res = prod3dm._checkemptydir( outputpattern )
    if not res['OK']:
      return res

    for localfile in glob.glob( outputpattern ):
      filename = os.path.basename( localfile )
      run_number = getRunNumber( filename, package )
      runpath = prod3dm._getRunPath( run_number )
      #lfn = os.path.join( path, 'Data', runpath, filename )
      lfn = os.path.join( path, outputType, runpath, filename )
      res = prod3dm.putAndRegister( lfn, localfile, filemetadata, package )
      if not res['OK']:
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

#!/usr/bin/env python
""" Collection of functions to download data files 
matching a given pattern / query.
    
         cta-prod3-get-matching-data.py JB, LA 2016
"""

# generic imports
import os

# DIRAC import Script
from DIRAC.Core.Base import Script
 
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s funcName' % Script.scriptName,
                                     'Arguments:',
                                     '  stepName: sub5, sub2sub5, HB9SCT',
                                     '\ne.g: %s sub5' % Script.scriptName
                                     ] ) )

Script.parseCommandLine()

# Other DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac

def downloadFile(lfn):
    """ Download a file using DMS
    Keyword arguments:
    lfn -- a logical file name
    """
    dirac = Dirac()
    res = dirac.getFile(lfn)
    if not res['OK']:
        DIRAC.gLogger.error ( res['Message'] )
        DIRAC.gLogger.error ( 'Could not download %s'%lfn )
        DIRAC.exit( -1 )
    return DIRAC.S_OK()
    
    
def getSub5():
    """ Download subarray-5 files corresponding to a list of subarray-2 files
    
    Keyword arguments:
    none -- none
    """
    DIRAC.gLogger.info('Get Subarray-5 files')
    # get JDL
    dirac = Dirac()
    resJDL = dirac.getJobJDL(os.environ['JOBID'] )    
    
    # get list of output files
    idata=resJDL['Value']['InputData']
    
    # dowload files
    for sub2 in idata:
        DIRAC.gLogger.debug("Input %s "%sub2)
        sub5=sub2.strip('\n').replace('subarray-2-nosct', 'subarray-5-nosct')
        downloadFile(sub5)
        
    return DIRAC.S_OK()

def getSub2Sub5():
    """ Download subarray-2 and 5 files corresponding to a list of subarray-1 files
    
    Keyword arguments:
    none -- none
    """
    DIRAC.gLogger.info('Get Subarray-2 and 5 files')
    # get JDL
    dirac = Dirac()
    resJDL = dirac.getJobJDL(os.environ['JOBID'] )    
    
    # get list of output files
    idata=resJDL['Value']['InputData']
    
    # dowload files
    for sub1 in idata:
        DIRAC.gLogger.debug("Input %s "%sub1)
        sub2=sub1.strip('\n').replace('subarray-1-nosct', 'subarray-2-nosct')
        downloadFile(sub2)        
        sub5=sub1.strip('\n').replace('subarray-1-nosct', 'subarray-5-nosct')
        downloadFile(sub5)
        
    return DIRAC.S_OK()

def getHB9SCT():
    """ Download SCT files corresponding to a list of HB9 merged files
    
    Keyword arguments:
    none -- none
    """
    DIRAC.gLogger.info('Get Subarray-5 files')
    # get JDL
    dirac = Dirac()
    resJDL = dirac.getJobJDL(os.environ['JOBID'] )    
    
    # get list of output files
    idata=resJDL['Value']['InputData']
    
    # dowload files
    for merged in idata:
        DIRAC.gLogger.debug("Input %s "%merged)
        sct=merged.strip('\n').replace('merged.simtel.gz', 'SCT.simtel.gz')
        downloadFile(sct)
        
    return DIRAC.S_OK()

# Main
def getMatchingFiles(args):
    """ simple wrapper to download data files matching a pattern
    
    Keyword arguments:
    args -- a list of arguments in order []
    """
    # check command line
    res=None
    if len(args)!=1:
        res=DIRAC.S_ERROR()
        res['Message'] = 'just give the function you wish to use: sub5, sub2sub5, HB9SCT'
        return res

    # now do something
    funcName = args[0] 

    # What shall we verify ?
    if funcName == "sub5":
        res=getSub5()
    elif funcName == "sub2sub5":
        res=getSub2Sub5()   
    elif funcName == "HB9SCT":
        res=getHB9SCT()   
    else:
        res=DIRAC.S_ERROR()
        res['Message'] = 'Function "%s" not known'% funcName
           
    return res

####################################################
if __name__ == '__main__':
  
  DIRAC.gLogger.setLevel('VERBOSE')
  args = Script.getPositionalArgs()
  try:    
    res = getMatchingFiles( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

#!/usr/bin/env python
""" Collection of simple functions to verify each of
    the main Prod3MCJob steps
    
         verifySteps.py JB, LA 2015
"""

# generic imports
import os, glob

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script
 
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s stepName' % Script.scriptName,
                                     'Arguments:',
                                     '  stepName: corsika, simtel, merging',
                                     '\ne.g: %s corsika' % Script.scriptName
                                     ] ) )

Script.parseCommandLine()

def cleanFiles( outputFiles ):
    """ Delete Local Files
    """
    DIRAC.gLogger.info( 'Deleting Local Files' )

    for afile in outputFiles:
      DIRAC.gLogger.warn( 'Remove local File %s' % ( afile ) )
      os.remove( afile )

    return DIRAC.S_OK()

def verifyCorsika( nbFiles = 6, minSize = 50. ):
    """ Verify a PROD3 corsika step
    
    Keyword arguments:
    nbFiles -- number of output files expected
    minSize -- minimum file size
    """
    DIRAC.gLogger.info('Verifying Corsika step')

    # get list of output files
    outputFiles=glob.glob('Data/corsika/run*/*corsika.gz')
    
    # check the number of output files
    N=len(outputFiles)
    if N != nbFiles :
        DIRAC.gLogger.error( 'Wrong number of Corsika files : %s instead of %s' % ( N, nbFiles ) )
        cleanFiles( outputFiles )
        return DIRAC.S_ERROR()
        
    # check the file size
    for afile in outputFiles:
        sizekb=os.path.getsize(afile)/1024.
        if sizekb < minSize:
            DIRAC.gLogger.error( '%s\n File size too small : %s < %s kb' % ( afile, sizekb, minSize ) )
            cleanFiles( outputFiles )
            return DIRAC.S_ERROR()

    return DIRAC.S_OK()


def verifySimtel(nbFiles=31, minSize=50.):
    """ Verify a PROD3 simtel step
    
    Keyword arguments:
    nbFiles -- number of output files expected
    minSize -- minimum file size
    """
    DIRAC.gLogger.info('Verifying Simtel step')
    # get list of output files
    outputFiles=glob.glob('Data/simtel_tmp/Data/*simtel.gz')
    
    # check the number of output files --- could be done by telescope type
    N=len(outputFiles)
    if N != nbFiles :
        DIRAC.gLogger.error( 'Wrong number of Simtel files : %s instead of %s' % ( N, nbFiles ) )
        cleanFiles( outputFiles )
        return DIRAC.S_ERROR()
        
    # check the file size --- could be done by telescope type
    for afile in outputFiles:
        sizekb=os.path.getsize(afile)/1024.
        if sizekb < minSize:
            DIRAC.gLogger.error( '%s\n File size too small : %s < %s kb' % ( afile, sizekb, minSize ) )
            cleanFiles( outputFiles )
            return DIRAC.S_ERROR()

    return DIRAC.S_OK()


def verifyMerging(nbFiles=10, minSize=5000.):
    """ Verify a PROD3 simtel merging step
    
    Keyword arguments:
    nbFiles -- number of output files expected
    minSize -- minimum file size
    """
    DIRAC.gLogger.info('Verifying Merging step')
    
    # get list of output files
    outputFiles=glob.glob('Data/sim_telarray/cta-prod3/0.0deg/Data/*simtel.gz')
    
    # check the number of output files --- could be done by telescope type
    N=len(outputFiles)
    if N != nbFiles :
        DIRAC.gLogger.error( 'Wrong number of Simtel Merged files : %s instead of %s' % ( N, nbFiles ) )
        cleanFiles( outputFiles )
        return DIRAC.S_ERROR()
        
    # check the file size --- could be done by telescope type
    for afile in outputFiles:
        sizekb=os.path.getsize(afile)/1024.
        if sizekb < minSize:
            DIRAC.gLogger.error( '%s\n File size too small : %s < %s kb' % ( afile, sizekb, minSize ) )
            cleanFiles( outputFiles )
            return DIRAC.S_ERROR()
            
    return DIRAC.S_OK()

def verifyAnalysisInputs( minSize = 50. ):
    """ Verify input files for analysis

    Keyword arguments:
    minSize -- minimum file size
    """
    DIRAC.gLogger.info( 'Verifying AnalysisInputs step' )

    # get list of output files
    outputFiles = glob.glob( './*simtel.gz' )

    # check the file size --- could be done by telescope type
    for afile in outputFiles:
        sizekb = os.path.getsize( afile ) / 1024.
        if sizekb < minSize:
            DIRAC.gLogger.warn( '%s\n File size too small : %s < %s kb' % ( afile, sizekb, minSize ) )
            DIRAC.gLogger.warn( 'Remove local File %s' % ( afile ) )
            os.remove( afile )

    return DIRAC.S_OK()

# Main
def verify(args):
    """ simple wrapper to put and register all PROD3 files
    
    Keyword arguments:
    args -- a list of arguments in order []
    """
    # check command line
    res=None
    if len(args)!=3:
        res=DIRAC.S_ERROR()
        res['Message'] = 'verify now requires nbFiles and fileSize as arguments'
        return res

    # now do something
    stepType = args[0] 
    nbFiles  = int(args[1])
    fileSize = float(args[2])

    # What shall we verify ?
    if stepType == "corsika":
        res=verifyCorsika(nbFiles, fileSize)
    elif stepType == "simtel":
        res=verifySimtel(nbFiles, fileSize)
    elif stepType == "merging":
        res=verifyMerging(nbFiles, fileSize)
    elif stepType == "analysisinputs":
        res = verifyAnalysisInputs( fileSize )
    else:
        res=DIRAC.S_ERROR()
        res['Message'] = 'Do not know how to verify "%s"'% stepType
           
    return res

####################################################
if __name__ == '__main__':
  
  DIRAC.gLogger.setLevel('VERBOSE')
  args = Script.getPositionalArgs()
  try:    
    res = verify( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
    else:
      DIRAC.gLogger.notice( 'Done' )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

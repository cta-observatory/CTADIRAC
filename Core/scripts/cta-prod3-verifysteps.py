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

Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s stepName' % Script.scriptName,
                                     'Arguments:',
                                     '  stepName: corsika, simtel, merging',
                                     '\ne.g: %s corsika' % Script.scriptName
                                     ] ) )

Script.parseCommandLine()

def clean_output_file(output_files):
    """ Delete Local Files
    """
    DIRAC.gLogger.info('Deleting Local Files')
    for afile in output_files:
        DIRAC.gLogger.warn('Remove local File %s' % afile)
        os.remove(afile)
    return DIRAC.S_OK()

def verify_corsika():
    """ Verify a generic Corsika log file
    """
    DIRAC.gLogger.info('Verifying Corsika log file')

    # get list of output files
    log_file = glob.glob('Data/corsika/run*/run*.log')
    if len(log_file) != 1:
        DIRAC.gLogger.error('"=== END OF RUN ===" not found!')
        return DIRAC.S_ERROR()

    # check EOR tag
    tag = '=== END OF RUN ==='
    content = open(log_file[0]).read()
    if content.find(tag)<0:
        DIRAC.gLogger.error('"%s" tag not found!'%tag)
        corsika_files = glob.glob('Data/corsika/run*/*corsika.*z*')
        if len(corsika_files)>0:
            clean_output_file(corsika_files)
        simtel_files = glob.glob('Data/sim_telarray/*/*/Data/*simtel.*z*')
        if len(simtel_files)>0:
            clean_output_file(simtel_files)
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
    outputFiles=glob.glob('Data/simtel_tmp/Data/*simtel.*z*')

    # check the number of output files --- could be done by telescope type
    N=len(outputFiles)
    if N != nbFiles :
        DIRAC.gLogger.error('Wrong number of Simtel files : %s instead of %s'%(N, nbFiles ) )
        clean_output_file(outputFiles )
        return DIRAC.S_ERROR()

    # check the file size --- could be done by telescope type
    for afile in outputFiles:
        sizekb=os.path.getsize(afile)/1024.
        if sizekb < minSize:
            DIRAC.gLogger.error('%s\n File size too small : %s < %s kb'%(afile, sizekb, minSize ) )
            clean_output_file(outputFiles )
            return DIRAC.S_ERROR()
    DIRAC.gLogger.info('Good files found:\n%s'%'\n'.join(outputFiles) )
    return DIRAC.S_OK()


def verifyMerging(nbFiles=10, minSize=5000.):
    """ Verify a PROD3 simtel merging step

    Keyword arguments:
    nbFiles -- number of output files expected
    minSize -- minimum file size
    """
    DIRAC.gLogger.info('Verifying Merging step')

    # get list of output files
    outputFiles=glob.glob('Data/sim_telarray/cta-prod3/0.0deg/Data/*simtel.*z*')

    # check the number of output files --- could be done by telescope type
    N=len(outputFiles)
    if N != nbFiles :
        DIRAC.gLogger.error('Wrong number of Simtel Merged files : %s instead of %s'%(N, nbFiles ) )
        clean_output_file(outputFiles )
        return DIRAC.S_ERROR()

    # check the file size --- could be done by telescope type
    for afile in outputFiles:
        sizekb=os.path.getsize(afile)/1024.
        if sizekb < minSize:
            DIRAC.gLogger.error('%s\n File size too small : %s < %s kb'%(afile, sizekb, minSize ) )
            clean_output_file(outputFiles )
            return DIRAC.S_ERROR()
    DIRAC.gLogger.info('Good files found:\n%s'%'\n'.join(outputFiles) )
    return DIRAC.S_OK()

def verifyAnalysisInputs(minSize = 50. ):
    """ Verify input files for analysis

    Keyword arguments:
    minSize -- minimum file size
    """
    DIRAC.gLogger.info('Verifying AnalysisInputs step')

    # get list of output files
    outputFiles = glob.glob('./*simtel.gz')

    # check the file size --- could be done by telescope type
    for afile in outputFiles:
        sizekb = os.path.getsize(afile ) / 1024.
        if sizekb < minSize:
            DIRAC.gLogger.warn('%s\n File size too small : %s < %s kb'%(afile, sizekb, minSize ) )
            DIRAC.gLogger.warn('Remove local File %s'%(afile ) )
            os.remove(afile )
            outputFiles.remove(afile) # remove from list of files processed
    DIRAC.gLogger.info('Good files found:\n%s'%'\n'.join(outputFiles) )
    return DIRAC.S_OK()

def verifyGeneric(nbFiles=1, minSize=50., path = 'Data/*'):
    """ Verify a PROD3 generic step

    Keyword arguments:
    nbFiles -- number of output files expected
    minSize -- minimum file size
    """
    DIRAC.gLogger.info('Verifying generic step output')

    # get list of output files
    outputFiles=glob.glob(path)

    # check the number of output files
    N=len(outputFiles)
    if N != nbFiles :
        DIRAC.gLogger.error('Wrong number of output files : %s instead of %s'%(N, nbFiles ) )
        clean_output_file(outputFiles )
        return DIRAC.S_ERROR()

    # check the file size
    for afile in outputFiles:
        sizekb=os.path.getsize(afile)/1024.
        if sizekb < minSize:
            DIRAC.gLogger.error('%s\n File size too small : %s < %s kb'%(afile, sizekb, minSize ) )
            clean_output_file(outputFiles )
            return DIRAC.S_ERROR()
    DIRAC.gLogger.info('Good files found:\n%s'%'\n'.join(outputFiles) )
    return DIRAC.S_OK()



def verify(args):
    """ simple wrapper to put and register all PROD3 files

    Keyword arguments:
    args -- a list of arguments in order []
    """
    # check command line
    if len(args)<1 :
        res=DIRAC.S_ERROR()
        res['Message'] = 'verify requires at least a step type'
        return res
    elif len(args)==1 :
        stepType = args[0]
    elif len(args)>3 :
        # now do something
        stepType = args[0]
        nbFiles  = int(args[1])
        fileSize = float(args[2])
        if len(args)==4 :
            path = args[3]

    # What shall we verify ?
    if stepType == "corsika":
        res = verify_corsika()
    elif stepType == "simtel":
        res = verifySimtel(nbFiles, fileSize)
    elif stepType == "merging":
        res = verifyMerging(nbFiles, fileSize)
    elif stepType == "analysisinputs":
        res = verifyAnalysisInputs(fileSize )
    elif stepType == "generic":
        res = verifyGeneric(nbFiles, fileSize, path )
    else:
        res = DIRAC.S_ERROR()
        res['Message'] = 'Do not know how to verify "%s"'% stepType
    return res

# Main
####################################################
if __name__ == '__main__':

  DIRAC.gLogger.setLevel('VERBOSE')
  args = Script.getPositionalArgs()
  try:
    res = verify(args )
    if not res['OK']:
      DIRAC.gLogger.error (res['Message'] )
      DIRAC.exit(-1 )
    else:
      DIRAC.gLogger.notice('Done')
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit(-1 )

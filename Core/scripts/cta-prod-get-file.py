#!/usr/bin/env python
from __future__ import print_function

__RCSID__ = "$Id$"

# generic imports
from multiprocessing import Pool
import signal
import os
import shutil

# DIRAC imports
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Bulk retrieval of a list of files from Grid storage to the current directory
Usage:
   %s <ascii file with lfn list>

""" % Script.scriptName)

Script.parseCommandLine(ignoreErrors=True)

# the client import must come after parseCommandLine
from DIRAC.Interfaces.API.Dirac import Dirac  # noqa

TEMPDIR = '.incomplete'


def sigint_handler(signum, frame):
    '''
    Raise KeyboardInterrupt on SIGINT (CTRL + C)
    This should be the default, but apparently Dirac changes it.
    '''
    raise KeyboardInterrupt()


def getfile(lfn):
    dirac = Dirac()
    print('Start downloading ' + lfn)
    res = dirac.getFile(lfn, destDir=TEMPDIR)

    if not res['OK']:
        print('Error downloading lfn:' + lfn)
        return res['Message']

    name = os.path.basename(lfn)
    os.rename(os.path.join('.incomplete', name), name)
    print('Successfully downloaded file:' + lfn)


if __name__ == '__main__':
    args = Script.getPositionalArgs()

    if len(args) == 1:
        infile = args[0]
    else:
        Script.showHelp()

    # put files currently downloading in a subdirectory
    # to know which files have already finished
    if not os.path.exists(TEMPDIR):
        os.makedirs(TEMPDIR)

    infileList = []
    with open(infile, 'r') as f:
        for line in f:
            infile = line.strip()
            if infile:
                infileList.append(infile)

    # see https://stackoverflow.com/a/35134329/3838691
    # ignore sigint before creating the pool,
    # so child processes inherit the setting, we will terminate them
    # if the master process catches sigint
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    p = Pool(10)
    # add the original handler back
    signal.signal(signal.SIGINT, sigint_handler)

    try:
        future = p.map_async(getfile, infileList)

        while not future.ready():
            future.wait(5)
    except (SystemExit, KeyboardInterrupt):
        print('Received SIGINT, waiting for subprocesses to terminate')
        p.close()
        p.terminate()
    finally:
        p.close()
        p.join()
        shutil.rmtree(TEMPDIR)

#!/usr/bin/env python
from __future__ import print_function

__RCSID__ = "$Id$"

# generic imports
from multiprocessing import Pool
import signal

# DIRAC imports
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Bulk retrieval of a list of files from Grid storage to the current directory
Usage:
   %s <ascii file with lfn list>

""" % Script.scriptName)

Script.parseCommandLine(ignoreErrors=True)


def getfile(lfn):
    dirac = Dirac()
    res = dirac.getFile(lfn)
    if not res['OK']:
        print('Error downloading lfn: ' + lfn)
        return res['Message']


if __name__ == '__main__':
    args = Script.getPositionalArgs()

    if len(args) == 1:
        infile = args[0]
    else:
        Script.showHelp()

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
    original_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    p = Pool(10)
    # add the original handler back
    signal.signal(signal.SIGINT, original_handler)

    try:
        future = p.map_async(getfile, infileList)

        while not future.ready():
            future.wait(5)
    except (SystemExit, KeyboardInterrupt):
        print('Received SIGINT, terminating')
        p.terminate()
    else:
        p.close()

    p.join()

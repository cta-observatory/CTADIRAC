#!/usr/bin/env python

import sys, os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

unit = 'GB'
Script.registerSwitch( "u:", "Unit=", "   Unit to use [default %s] (MB,GB,TB,PB)" % unit )

Script.setUsageMessage( """
Get the size of the standard output of a regular prod using the standardized config name

Usage:
   %s <prodName>
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = False )
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "u" or switch[0].lower() == "unit":
    unit = switch[1]
scaleDict = { 'MB' : 1000 * 1000.0,
              'GB' : 1000 * 1000 * 1000.0,
              'TB' : 1000 * 1000 * 1000 * 1000.0,
              'PB' : 1000 * 1000 * 1000 * 1000 * 1000.0}
if not unit in scaleDict.keys():
  gLogger.error( "Unit must be one of MB,GB,TB,PB" )
  DIRAC.exit( 2 )
scaleFactor = scaleDict[unit]

args = Script.getPositionalArgs()
prodName=args[0]
mct=prodName[-2:]
mcname=''
if mct in ['gn','gs']:
  mcname='gamma_ptsrc'
elif mct in ['dn','ds']:
  mcname='gamma'
elif mct in ['en','es']:
  mcname='electron'
elif mct in ['pn','ps']:
  mcname='proton'
else:
  gLogger.error('Uknown config extension: ',mct) 
  DIRAC.exit( 2 )

gLogger.notice('Working with prodName ',prodName)

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
cat = FileCatalog()

BASE_PROD_DIR='/vo.cta.in2p3.fr/MC/PROD2/'

topMCDir=os.path.join(BASE_PROD_DIR,prodName,'prod-2_13112014_corsika',mcname)
res=cat.listDirectory(topMCDir)
NB_FILES_DIR={}
TOTAL_SIZE_DIR={}
gLogger.notice('Looking for Data files...')
subdirs=res['Value']['Successful'].values()[0]['SubDirs'].keys()
for adir in subdirs:
    tag=adir.split('/')[-1].split('_')[-1]
    print tag,
    NB_FILES_DIR[tag]=0
    TOTAL_SIZE_DIR[tag]=0
    subres=cat.listDirectory(os.path.join(adir,'Data'))
    for xxx in subres['Value']['Successful'].values()[0]['SubDirs'].keys():
        sizeDir=cat.getDirectorySize(xxx)['Value']['Successful']
        for key,val in sizeDir.items():
            print val['Files'],
            NB_FILES_DIR[tag]+=val['Files']
            TOTAL_SIZE_DIR[tag]+=val['TotalSize']
    print

gLogger.notice('\nData found:')
skeys=NB_FILES_DIR.keys()
skeys.sort()
nbfiles=0
nbGB=0
for tag in skeys:
    print '%s\t%s\t%.1f GB'%(tag, NB_FILES_DIR[tag], TOTAL_SIZE_DIR[tag]/scaleFactor)
    nbfiles+=NB_FILES_DIR[tag]
    nbGB+=TOTAL_SIZE_DIR[tag]/scaleFactor

gLogger.notice('\nTotal: %s files for %.1f GB'%(nbfiles,nbGB))
print '\nFor Wiki page:'
txt=''
for tag in skeys:
    txt+='(%s,%s) '%(tag, NB_FILES_DIR[tag])
print txt

DIRAC.exit( 0 )


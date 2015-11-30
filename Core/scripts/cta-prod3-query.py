#!/usr/bin/env python

"""
  Select PROD3 files matching MetaData conditions (see options)
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )

Script.registerSwitch( "", "site=", "e.g. Paranal" )
Script.registerSwitch( "", "particle=", "e.g. gamma,gamma-diffuse,electron,proton" )
Script.registerSwitch( "", "tel_sim_prog=", "e.g. simtel" )
Script.registerSwitch( "", "tel_sim_prog_version=", "e.g. 2015-07-21" )
Script.registerSwitch( "", "analysis_prog=", "e.g. evndisp" )
Script.registerSwitch( "", "analysis_prog_version=", "e.g. prod3_d20150903b" )
Script.registerSwitch( "", "training_type=", "e.g. gamma, gamma-diffuse" )
Script.registerSwitch( "", "array_layout=", "e.g. hex,square" )
Script.registerSwitch( "", "thetaP=", "e.g. 20" )
Script.registerSwitch( "", "phiP=", "e.g. 0,180" )
Script.registerSwitch( "", "sct=", "e.g. True,False" )
Script.registerSwitch( "", "outputType=", "e.g. Data,Log" )


Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

if len( Script.getUnprocessedSwitches() ) == 0:
  Script.showHelp()

# ## Set default for MCCampaign
metaDict = {}
metaDict['MCCampaign'] = 'PROD3'

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "site":
    metaDict['site'] = switch[1]
  elif switch[0].lower() == "particle":
    metaDict['particle'] = switch[1]
  elif switch[0].lower() == "tel_sim_prog":
    metaDict['tel_sim_prog'] = switch[1]
  elif switch[0].lower() == "tel_sim_prog_version":
    metaDict['tel_sim_prog_version'] = switch[1]
  elif switch[0].lower() == "analysis_prog":
    metaDict['analysis_prog'] = switch[1]
  elif switch[0].lower() == "analysis_prog_version":
    metaDict['analysis_prog_version'] = switch[1]
  elif switch[0].lower() == "array_layout":
    metaDict['array_layout'] = switch[1]
  elif switch[0].lower() == "thetap":
    metaDict['thetaP'] = switch[1]
  elif switch[0].lower() == "phip":
    metaDict['phiP'] = switch[1]
  elif switch[0].lower() == "training_type":
    metaDict['training_type'] = switch[1]
  elif switch[0].lower() == "training_type":
    metaDict['sct'] = switch[1]
  elif switch[0].lower() == "outputtype":
    metaDict['outputType'] = switch[1]

fc = FileCatalogClient()

res = fc.findFilesByMetadata( metaDict, path = '/', rpc = '', url = '', timeout = 300 )

if not res['OK']:
  DIRAC.gLogger.error ( res['Message'] )
  DIRAC.exit( -1 )
else:
  for lfn in res['Value']:
    print lfn


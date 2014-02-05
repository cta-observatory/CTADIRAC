#!/usr/bin/env python

"""
  Select Files matching the given MetaData conditions
"""

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "MCCampaign=", "PROD2" )
Script.registerSwitch( "", "particle=", "gamma/gamma_ptsrc/proton/electron" )
Script.registerSwitch( "", "simtelArrayConfig=", "STD/NSBX3/SCMST/4MSST/SCSST/ASTRI/NORTH" )
Script.registerSwitch( "", "outputType=", "Data/Log/Histo" )
Script.registerSwitch( "", "viewCone=", "10/0" )
Script.registerSwitch( "", "corsikaprodversion=", "prod-2_21122012_corsika/prod-2_06052013_corsika/prod-2_22072013_corsika/prod-2_15122013_corsika" )
Script.registerSwitch( "", "simtelArrayProdVersion=", "prod-2_06052013_simtel/prod-2_06052013_sc3_simtel/prod-2_22072013_simtel/prod-2_15122013_simtel/prod-2_15122013_simtel_sc3" )
Script.registerSwitch( "", "dstProdVersion=", "prod-2_15122013_dst/prod-2_15122013_sc3_dst" )
Script.registerSwitch( "", "altitude=", "1640/1650/1655/2434/2290/2662/3600" )
Script.registerSwitch( "", "thetaP=", "20/40" )
Script.registerSwitch( "", "phiP=", "0/180" )
Script.registerSwitch( "", "energyInfo=", "-2.0_3.0-3.30E3/-2.0_3.0-330E3/-2.0_4.0-10E3/-2.0_4.0-600E3/-2.0_6.0-660E3/-2.0_8.0-1200E3" )
#Script.registerSwitch( "", "runNumSeries=", "runNumSeries" )
Script.registerSwitch( "", "offset=", "0" )
Script.registerSwitch( "", "prodName=", "prodName" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

#Default values
particle = None
MCCampaign = None
prodName = None
outputType = None
viewCone = None
corsikaProdVersion = None
simtelArrayProdVersion = None
dstProdVersion = None
simtelArrayConfig = None
thetaP = None
altitude = None
phiP = None
energyInfo = None
#runNumSeries = None
offset = None
metaDict = {}

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "particle":
    particle = switch[1]
    metaDict['particle'] = particle
  elif switch[0].lower() == "mccampaign":
    MCCampaign = switch[1]
    metaDict['MCCampaign'] = MCCampaign
  elif switch[0].lower() == "prodname":
    prodName = switch[1]
    metaDict['prodName'] = prodName
  elif switch[0].lower() == "outputtype":
    outputType = switch[1]
    metaDict['outputType'] = outputType
  elif switch[0].lower() == "viewcone":
    viewCone = switch[1]
    metaDict['viewCone'] = viewCone
  elif switch[0].lower() == "corsikaprodversion":
    corsikaProdVersion = switch[1]
    metaDict['corsikaProdVersion'] = corsikaProdVersion
  elif switch[0].lower() == "thetap":
    thetaP = switch[1]
    metaDict['thetaP'] = thetaP
  elif switch[0].lower() == "altitude":
    altitude = switch[1]
    metaDict['altitude'] = altitude
  elif switch[0].lower() == "phip":
    phiP = switch[1]
    metaDict['phiP'] = phiP
  elif switch[0].lower() == "simtelarrayprodversion":
    simtelArrayProdVersion = switch[1]
    metaDict['simtelArrayProdVersion'] = simtelArrayProdVersion
  elif switch[0].lower() == "dstprodversion":
    dstProdVersion = switch[1]
    metaDict['dstProdVersion'] = dstProdVersion
  elif switch[0].lower() == "energyinfo":
    energyInfo = switch[1]
    metaDict['energyInfo'] = energyInfo
  #elif switch[0].lower() == "runnumseries":
   # runNumSeries = switch[1]
    #metaDict['runNumSeries'] = runNumSeries
  elif switch[0].lower() == "offset":
    offset = switch[1]
    metaDict['offset'] = offset
  elif switch[0].lower() == "simtelarrayconfig":
    simtelArrayConfig = switch[1]
    metaDict['simtelArrayConfig'] = simtelArrayConfig

if metaDict == {}:
  Script.showHelp()

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

fcD = FileCatalog('DIRACFileCatalog')

fc = FileCatalogClient()

#print "Metadata query conditions are:"
#print metaDict

result = fc.findFilesByMetadata(metaDict,path='/',rpc='',url='',timeout=300)

if not result['OK']:
  print 'ERROR %s' % result['Message']
  exitCode = 2
else:
  for f in result['Value']:
    print f
  
DIRAC.exit( exitCode )

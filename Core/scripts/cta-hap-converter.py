#!/usr/bin/env python
import DIRAC
import os

def setInfile( optionValue ):
  global infile
  infile = optionValue
  return DIRAC.S_OK()

def setOutfile( optionValue ):
  global outfile
  outfile = optionValue
  return DIRAC.S_OK()
  
def setConfigfile( optionValue ):
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  global configfile
  configfile = os.path.join( localArea(),
                         'HAP/%s/config/%s' % (version, optionValue))
  return DIRAC.S_OK()
  
def setVersion( optionValue ):
  global version
  version = optionValue
  return DIRAC.S_OK()

def main():

  from DIRAC.Core.Base import Script

  Script.registerSwitch( "I:", "infile=", "Input file", setInfile )
  Script.registerSwitch( "O:", "outfile=", "Output file", setOutfile )
  Script.registerSwitch( "T:", "tellist=", "Configuration file", setConfigfile )
  Script.registerSwitch( "V:", "version=", "HAP Version", setVersion )

  Script.parseCommandLine( ignoreErrors = True )

  if infile == None or outfile == None or configfile == None or version == None:
    Script.showHelp()
    DIRAC.exit( -1 )


  DIRAC.gLogger.notice( 'Executing Hap Converter Application' )

  from CTADIRAC.Core.Workflow.Modules.HapConverter import HapConverter
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea

  hc = HapConverter()

  HapPack = 'HAP/' + version + '/HAP'

  packs = ['HESS/v0.1/lib','HESS/v0.1/root',HapPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        continue
    if localArea:
      if checkSoftwarePackage( package, localArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Local Area:', package )
        continue
      if installSoftwarePackage( package, localArea() )['OK']:
        continue
    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    return DIRAC.S_ERROR( '%s not available' % package )

  hc.setSoftwarePackage(HapPack)

  hc.hapArguments = ['-file', infile, '-o', outfile, '-tellist', configfile ]

  res = hc.execute()

  if not res['OK']:
    DIRAC.exit( -1 )

  DIRAC.exit()



if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



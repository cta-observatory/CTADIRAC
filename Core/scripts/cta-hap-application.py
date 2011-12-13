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
                       'HAP/v0.1/config/%s' % optionValue)
  return DIRAC.S_OK()


def main():

  from DIRAC.Core.Base import Script

  Script.registerSwitch( "I:", "infile=", "Input file", setInfile )
  Script.registerSwitch( "O:", "outfile=", "Output file", setOutfile )
  Script.registerSwitch( "T:", "tellist=", "Configuration file", setConfigfile )

  Script.parseCommandLine( ignoreErrors = True )

  if infile == None or outfile == None or configfile == None:
    Script.showHelp()
    DIRAC.exit( -1 )


  DIRAC.gLogger.notice( 'Executing a Hap Application' )

  from CTADIRAC.Core.Workflow.Modules.HapApplication import HapApplication
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea


  ha = HapApplication()

  package =  'HAP/v0.1/HAP'
  if checkSoftwarePackage( package, localArea() )['OK']:
    DIRAC.gLogger.notice( 'Package found in Local Area:', package )
  else:
    installSoftwarePackage( package, localArea() )

  ha.setSoftwarePackage(package)

  ha.hapArguments = ['-file', infile, '-o', outfile, '-tellist', configfile ]

  res = ha.execute()

  if not res['OK']:
    DIRAC.exit( -1 )

  DIRAC.exit()




if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



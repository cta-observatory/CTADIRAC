#!/usr/bin/env python
import DIRAC

def main():

  from DIRAC.Core.Base import Script
  Script.registerSwitch( "infile", "infile", "Input file" )
  Script.registerSwitch( "outfile", "outfile", "Output file" )
  Script.registerSwitch( "tellist", "tellist", "Configuration file" )

  Script.parseCommandLine( ignoreErrors = True )
#  Script.parseCommandLine()

  from CTADIRAC.Core.Workflow.Modules.HapApplication import HapApplication
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  import os
   
  DIRAC.gLogger.notice( 'Executing a Hap Application' )

  args = Script.getPositionalArgs()
  DIRAC.gLogger.notice( 'Arguments:', args )

  ha = HapApplication()
  ha.setSoftwarePackage('HAP/v0.1/HAP') 



#  ha.setSoftwarePackage() 
  # There is a bug in the Job.py class that produce a duplicated is the first argument

  if args[0] == 'jobDescription.xml' or args[0] == 'eventio_cta':
    args=args[1:]


  args[2] = os.path.join( localArea(),
                       'HAP/v0.1/config/%s' % args[2])

  args[0] = '-file ' + args[0]
  args[1] = '-o ' + args[1]
  args[2] = '-tellist ' + args[2] 


  ha.hapArguments = args

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

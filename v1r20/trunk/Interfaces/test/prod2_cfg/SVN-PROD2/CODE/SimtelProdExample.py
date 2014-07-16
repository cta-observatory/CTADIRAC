#!/usr/bin/env python
"""
  Submit a Simtel Example Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Input File List',
                                     '  storageElement: Storage Element'] ) )
Script.parseCommandLine()

import os

def SimtelProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.SimtelProdJob import SimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  if (len(args)<1 or len(args)>3):
    Script.showHelp()

  if (len(args)>=2):
    storage_element = args[1]
  else :
    storage_element = 'LUPM-Disk'

  LFN_file = args[0]
  f = open(LFN_file,'r')

  infileLFNList = []
  for line in f:
    infileLFN = line.strip()
    if line!="\n":
      infileLFNList.append(infileLFN)

  j = SimtelProdJob()

  j.setVersion('prod-2_06052013')
  cfg = 'cta-prod2' #this is hardcoded in generic_run
  #simtelArrayConfig, passed as argument to this script or defaulted to STD, refers to the MD in the catalog and the actual storage path.
  simtelArrayConfig = "STD"

  if (len(args)>=3):
    simtelArrayConfig = args[2]
    if simtelArrayConfig == "SCMST":
      cfg=='cta-prod2-sc3'
      j.setVersion('prod-2_06052013_sc3')
    elif simtelArrayConfig == "4MSST":
      cfg="cta-prod2-4m-dc"
    elif simtelArrayConfig == "SCSST":
      cfg="cta-prod2-sc-sst"
    elif simtelArrayConfig == "ASTRI":
      cfg="cta-prod2-astri"
    elif simtelArrayConfig == "NSBX3":
      cfg="cta-prod2"
    elif simtelArrayConfig == "STD":
      cfg="cta-prod2"
    else :
      print "arrayConfig argument %s incorrect"%simtelArrayConfig
      exit(1)

  j.setExecutable(cfg)

  j.setParametricInputData(infileLFNList)

  #From Ricardo, to manage smooth loading
  j.setType( 'CorsikaRepro' )

  name = 'repro_' + simtelArrayConfig
  j.setName(name)
  j.setJobGroup(name)

  j.setInputSandbox( [ 'fileCatalog.cfg','CTADIRAC','cta-simtelprodold.py','grid_prod2-repro.sh','SVN-PROD2_rev1868'] )

  j.setParameters(['fileCatalog.cfg','-D',storage_element,'-S',simtelArrayConfig])
  j.setOutputSandbox( ['simtel.log'])

  j.setCPUTime(100000)

  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    SimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

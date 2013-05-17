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
                                     '  storageElement: Storage Element',
                                     ' reprocessing configuration (STD,NSBX3,SCMST,SCSST,4MSST,ASTRI,6INROW)'] ) )
Script.parseCommandLine()

import os

def SimtelProdExample( args = None ) :
  
  from CTADIRAC.Interfaces.API.SimtelProdJob import SimtelProdJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  if (len(args)>=2):
    storage_element = args[1]
  else:
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
  #cfg = 'cta-prod2' #this is hardcoded in generic_run
  #simtelArrayConfig, passed as argument to this script or defaulted to STD, refers to the MD in the catalog and the actual storage path.
  simtelArrayConfig = "STD"

  if (len(args)>=3):
    if args[2] not in ['STD','NSBX3','SCMST','SCSST','4MSST','ASTRI','6INROW']:
      print "arrayConfig argument %s incorrect"%args[2]
      Script.showHelp()

    simtelArrayConfig = args[2]

  j.setParametricInputData(infileLFNList)
  j.setType( 'CorsikaRepro' )

  name = 'repro_' + simtelArrayConfig
  j.setName(name)
  j.setJobGroup(name)

  j.setInputSandbox( [ 'fileCatalog.cfg','grid_prod2-repro.sh','LFN:/vo.cta.in2p3.fr/user/j/johann.cohen-tanugi/PROD2/SVN-PROD2_rev1869.tar.gz'] )

  j.setParameters(['fileCatalog.cfg','-D',storage_element,'-S',simtelArrayConfig])
  j.setOutputSandbox( ['simtel.log'])
  j.setCPUTime(100000)
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  if len(args)!=3:
    Script.gLogger.notice('need 3 args : 1/file of corsika lfns 2/Storage Element 3/Config')
    Script.showHelp()

  try:
    SimtelProdExample( args )
  except Exception:
    Script.gLogger.exception()

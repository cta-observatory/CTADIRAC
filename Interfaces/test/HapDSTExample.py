#!/usr/bin/env python
"""
  Submit an Example HapJob
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [Site] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Requested Site' ] ) )
Script.parseCommandLine()

import os

def HapDSTExample( destination = None ) :
  from CTADIRAC.Interfaces.API.HapDSTJob import HapDSTJob
  from DIRAC.Interfaces.API.Dirac import Dirac

### general options ###############
  HapVersion = 'v0.18'

  infileLFNList = [
  'LFN:/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/Rawdata/gamma/raw_gamma_run283000.root',
  'LFN:/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/Rawdata/gamma/raw_gamma_run283001.root']

  for infileLFN in infileLFNList:
    infile = os.path.basename(infileLFN)
    RunNum = infile.split( 'run' )[1].split('.root')[0]
    tellist = 'array-E.lis'

    general_opts = ['-V', HapVersion]
### make_CTA_DST.C options ###############
    make_CTA_DST_opts =  ['-R',RunNum ,'-I',infile,'-T',tellist]

    opts =  general_opts + make_CTA_DST_opts

    j = HapDSTJob(opts)

    if destination:
      j.setDestination( destination )

    j.setInputSandbox( [ 'passphrase','check_dst0.csh','check_dst2.csh'] )
    j.setOutputSandbox( ['make_CTA_DST.log','CheckDST.log'])
    jobName = 'DSTrun_' + RunNum
    j.setName(jobName)
    j.setInputData([infileLFN])
    j.setOutputData(['dst*.root'])

    j.setCPUTime(100000)
    Script.gLogger.info( j._toJDL() )
    Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    HapDSTExample( args )
  except Exception:
    Script.gLogger.exception()



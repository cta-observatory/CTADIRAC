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

def HapConverterParamExample( destination = None ) :
  from CTADIRAC.Interfaces.API.HapConverterJob import HapConverterJob
  from DIRAC.Interfaces.API.Dirac import Dirac

### general options ###############
  HapVersion = 'v0.16'

  infileLFNList = [
  'LFN:/vo.cta.in2p3.fr/Simulation/sim_telarray/Prod1S_PS/2000/gamma/20/90/spectrum_-2.0/0.003_300/pointlike/cta-prod1/0.0deg/Data/run283xxx/gamma_20deg_90deg_run283000___cta-prod1_desert.simhess.gz',
  'LFN:/vo.cta.in2p3.fr/Simulation/sim_telarray/Prod1S_PS/2000/gamma/20/90/spectrum_-2.0/0.003_300/pointlike/cta-prod1/0.0deg/Data/run283xxx/gamma_20deg_90deg_run283001___cta-prod1_desert.simhess.gz']


  fileout_prefix = 'raw_' 
  tellist = 'array-E.lis'

  general_opts = ['-V', HapVersion]
  eventio_cta_opts = ['--outfile',fileout_prefix,'--tellist',tellist,'--pixelslices','true']

  opts =  general_opts + eventio_cta_opts

  j = HapConverterJob(opts)

  if destination:
    j.setDestination( destination )

  j.setInputSandbox( [ 'passphrase','check_raw.csh','CTADIRAC','cta-hap-converter.py'] )
  j.setOutputSandbox( ['eventio_cta.log','Open_Raw.log'])
  j.setParametricInputData(infileLFNList)

  j.setName('eventio')

  j.setOutputData(['raw_*.root'])

  j.setCPUTime(100000)
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    HapConverterParamExample( args )
  except Exception:
    Script.gLogger.exception()



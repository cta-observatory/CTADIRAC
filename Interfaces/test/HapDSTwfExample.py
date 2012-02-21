#!/usr/bin/env python
"""
  Submit an Example Hap Converter Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [Site] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Requested Site' ] ) )
Script.parseCommandLine()

import os

def HapDSTwfExample( destination = None ) :
  from CTADIRAC.Interfaces.API.HapDSTwfJob import HapDSTwfJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  HapVersion = 'v0.3'
  config = 'array-E.lis'
  infileLFN = 'LFN:/vo.cta.in2p3.fr/Simu2/v_Leeds/Data/sim_hessarray/cta-ultra3/0.0deg/Data/proton_20deg_90deg_run89580___cta-ultra3_desert.simhess.gz'
  infile = os.path.basename(infileLFN)
  fileout = infile.replace('simhess.gz',os.path.splitext(os.path.basename(config))[0] + '.root')
  Nevents = '100000' ####### Nevents for the make_CTA_DST.C macro
   
  j = HapConverterJob('eventio_cta',['-I',infile,'-O', fileout,'-N',Nevents])   

  j.setVersion(HapVersion)
  j.setConfig(config)

  if destination:
    j.setDestination( destination )

  j.setInputSandbox( [ 'passphrase' ] )
  j.setName('HapDSTwf')
  j.setInputData([infileLFN])
  j.setOutputData(fileout)
### add dst output #####################
  j.setPlatform( "gLite-HighMem" )

  Script.gLogger.info( j._toJDL() )

  return Dirac().submit( j )

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  ret = HapDSTwfExample( args )

  if ret['OK']:
    Script.gLogger.notice( 'Submitted Job:', ret['Value'] )
  else:
    Script.gLogger.error( ret['Message'] )


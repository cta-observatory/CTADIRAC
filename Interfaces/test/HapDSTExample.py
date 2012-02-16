#!/usr/bin/env python
"""
  Submit an Example HapDST Job
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

  infileLFN = 'LFN:/vo.cta.in2p3.fr/HAPtest/rawdata/proton_20deg_90deg_run89580___cta-ultra3_desert.array-E.root'

  RunNum = 89580
  FileName = os.path.basename(infileLFN)
  ArrayConfig = 'array-E.lis'
  Nevent=100000
  fileout = '/tmp/dst_CTA_0000' + str(RunNum) + '.root'
  toCompile = False 
   
  j = RootJob( 'make_CTA_DST.C', [RunNum,FileName,ArrayConfig,Nevent], toCompile )

  if destination:
    j.setDestination( destination )

  j.setInputSandbox( [ 'passphrase' ] )

  j.setInputData([infileLFN])
  j.setOutputData(fileout)
  j.setCPUTime(200000)
  j.setPlatform( "gLite-HighMem" )

  Script.gLogger.info( j._toJDL() )

  return Dirac().submit( j )



if __name__ == '__main__':

  args = Script.getPositionalArgs()

  ret = HapDSTExample( args )
  if ret['OK']:
    Script.gLogger.notice( 'Submitted Job:', ret['Value'] )
  else:
    Script.gLogger.error( ret['Message'] )

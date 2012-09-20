#!/usr/bin/env python
"""
  Submit an Example Hap Job
"""
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [Site] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Site:     Requested Site' ] ) )
Script.parseCommandLine()

import os

def HapLookupExample( destination = None ) :
  from CTADIRAC.Interfaces.API.HapLookupJob import HapLookupJob
  from DIRAC.Interfaces.API.Dirac import Dirac

  HapVersion = 'v0.16'
####### DoCtaIrf option values for Table prod #######################
  AnalysisType = 'ScaleParam'  
  EnergyMethod = 'NoEnergy'
  CutsConfig = 'mva_40pe_Wm_E_MST_PSFA_cta0909'
  RunList = 'dstrun'
  Zenith = '20'
  Offset = '0'
  Array = 'array-E.lis'
  ParticleType = 'gamma'
 #### for Energy prod ##################
#  AnalysisType = 'Energy'
#  EnergyMethod = 'Oak'

  infileLFNList = [
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283000.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283001.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283002.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283003.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283004.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283005.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283006.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283007.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283008.root',
'/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/DST/gamma/dst_CTA_00283009.root']


  general_opts = ['-V', HapVersion]

  DoCtaIrf_opts = ['-A',  AnalysisType,'-C', CutsConfig ,'-R', RunList, '-Z', Zenith, '-O', Offset, '-T', Array,'-M', EnergyMethod,'-P', ParticleType]

  opts = general_opts + DoCtaIrf_opts

  j = HapApplicationJob(opts)

  if destination:
    j.setDestination( destination )

  j.setInputSandbox( [ 'LFN:/vo.cta.in2p3.fr/user/a/arrabito/HAP/mini-prod3/conf/v0.1/AnalysisConfig.tar.gz','passphrase'] )

  j.setOutputSandbox( ['DoCtaIrf.log'])

  j.setName(AnalysisType)

  j.setInputData(infileLFNList)
  j.setInputDataPolicy('Protocol')

  if (AnalysisType=='ScaleParam'):
    outfile = 'ScaleInfo_' + RunList + '.root'
  elif (AnalysisType=='Energy'):
    outfile = 'EnergyCalib_' + RunList + '.root'

  j.setOutputData([outfile]) 

  j.setCPUTime(100000) 
  Script.gLogger.info( j._toJDL() )
  Dirac().submit( j )


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    HapLookupExample( args )
  except Exception:
    Script.gLogger.exception()



""" Remove Prod3 MC sw
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s package version' % Script.scriptName,
                                     'Arguments:',
                                     '  package: corsika_simhessarray',
                                     '  version: 2015-07-13',
                                     '  site: LCG.IN2P3-CC.fr',
                                     '\ne.g: %s corsika_simhessarray 2015-07-13 LCG.IN2P3-CC.fr' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

#########################################################

def cleanProd3sw( args = None ):
  """ Simple wrapper to remove Software package
  """
 
  # get arguments
  package = args[0]
  version = args[1]
  site = args[2]
  
# ##
  job = Job()
  dirac = Dirac()

  Step = job.setExecutable( './cta-cleansw.py',
                            arguments = '%s %s' % ( package, version ), \
                            logFile = 'cleanSoftware_Log.txt' )
  Step['Value']['name'] = 'Step_cleanSoftware'
  Step['Value']['descr_short'] = 'clean Software'

  # override for testing
  job.setName( 'CleanProd3Sw' )
  
  # send job at Lyon CC
  job.setDestination( [site] )
  
  # run job

  # res = dirac.submit( job, "local" )
  res = dirac.submit( job )

  Script.gLogger.notice( 'Submission Result: ', res )
  
  return DIRAC.S_OK ('Done')


#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  try:
    if ( len( args ) != 3 ):
      Script.showHelp()
    res = cleanProd3sw( args )
    if not res['OK']:
      DIRAC.gLogger.error ( res['Message'] )
      DIRAC.exit( -1 )
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )

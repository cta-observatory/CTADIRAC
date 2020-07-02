import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ 'Get storage elements usage of production SEs',
                                    'Usage:',
                                     '%s <file with output of lcg-infosites --vo vo.cta.in2p3.fr se>' % Script.scriptName,
                                     '\ne.g: %s ccsrm02.in2p3.fr (default is all SEs)' % Script.scriptName
                                     ] ) )


Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

args = Script.getPositionalArgs()

if len( args ) > 2:
  Script.showHelp()

## Default list
if len(args) == 1:
  opsHelper = Operations()
  SEList = opsHelper.getValue( 'ProductionSEs/Hosts' , [] )
else:
  SEList = args[:2]

sedict = {}
for SE in SEList:
  sedict[SE] = []

fields = ['SE', 'Available(TB)', 'Used(TB)', 'Total(TB)', 'Used(%)' ]
records = []

fp = open(args[0],"r")

for se in fp:
  if len( se.split() ) == 4:
    spacedict = {}
    SE = se.split()[3]
    if SE in SEList and se.split()[0] != 'n.a' and se.split()[1] != 'n.a':
      # ## convert into TB
      available = float( se.split()[0] ) / 1e9
      used = float( se.split()[1] ) / 1e9
      spacedict['Available'] = available
      spacedict['Used'] = used
      spacedict['Total'] = available + used
      sedict[SE].append(spacedict)

for SE in SEList:
  for spacedict in sedict[SE]:
    available = '%.1f' % spacedict['Available'] 
    used = '%.1f' % spacedict['Used']
    total = '%.1f' % spacedict['Total']
    fraction_used = spacedict['Used'] / spacedict['Total'] * 100
    fraction_used = '%.1f' % fraction_used
    records.append( [SE, available, used, total, fraction_used] )

printTable( fields, records )


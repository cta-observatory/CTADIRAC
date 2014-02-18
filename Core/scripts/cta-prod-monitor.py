#!/bin/env python
"""
  Simple terminal job monitoring
  
"""

owner=""
jobGroup=""
nHours=24

def highlight(string):
    return '\x1b[31;1m%s\x1b[0m' % (string)
    
import os, sys, copy
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Time import toString, date, day
import datetime

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [options]' % Script.scriptName,
				     'e.g.:',
				     '  %s --owner=bregeon --hours=24' % Script.scriptName] ) )


Script.registerSwitch( "", "owner=", "the job owner" )
Script.registerSwitch( "", "jobGroup=", "the job group" )
Script.registerSwitch( "", "hours=", "Get status for jobs of the last n hours" )
Script.registerSwitch( "", "failed=", "1 or 0 : Save or not failed jobs in \"failed.txt\"" )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "owner":
    owner = switch[1]
  elif switch[0].lower() == "jobgroup":
    jobGroup = switch[1]
  elif switch[0].lower() == "hours":
    nHours = int(switch[1])
  elif switch[0].lower() == "failed":
    SaveFailed = int(switch[1])

#print owner, jobGroup, nHours

# Start doing something
# import Dirac here (and not on the top of the file) if you don't want to get into trouble
from DIRAC.Interfaces.API.Dirac  import Dirac
dirac = Dirac()


# dirac.selectJobs( status='Failed', owner='paterson', site='LCG.CERN.ch')
# owner=owner, date=jobDate
onehour = datetime.timedelta(hours = 1)
now=datetime.datetime.now()

results=dirac.selectJobs(jobGroup=jobGroup, owner=owner, date=now-nHours*onehour)
if not results.has_key('Value'):
    print("No job found for group \"%s\" and owner \"%s\" in the past %s hours"%
       (jobGroup, owner, nHours))
    sys.exit(0)

# Found some jobs, print information
jobsList=results['Value']
print("%s jobs found for group \"%s\" and owner \"%s\" in the past %s hours\n"%
       (len(jobsList), jobGroup, owner, nHours))

status=dirac.status(jobsList)

# for details
#print dirac.getJobSummary(3075536)

# print out my favourite tables
BASE_STATUS_DIR={'Matched':0, 'Waiting':0, 'Running':0, 'Failed':0, 'Stalled':0, 'Checking':0, 'Done':0, 'Completed':0, 'Total':0}
SitesDict={}
StatusDict=copy.copy(BASE_STATUS_DIR)

for job in jobsList:
#    print job, status['Value'][int(job)]
    site=status['Value'][int(job)]['Site']
    minstatus=status['Value'][int(job)]['MinorStatus']
    majstatus=status['Value'][int(job)]['Status']
    if majstatus not in StatusDict.keys():
        print 'Add %s to BASE_STATUS_DIR'%majstatus
	sys.exit(1)
    StatusDict[majstatus]+=1
    StatusDict['Total']+=1
    if site not in SitesDict.keys():
        if site.find('.')==-1:
	    site='    None' # note that blank spaces are needed
        SitesDict[site]=copy.copy(BASE_STATUS_DIR)
        SitesDict[site][majstatus]=1
        SitesDict[site]["Total"]=1
    else:
        SitesDict[site]["Total"]+=1
        if majstatus not in SitesDict[site].keys():
            SitesDict[site][majstatus]=1
        else:
            SitesDict[site][majstatus]+=1

print "%12s\tWaiting\tRunning\tFailed\tStalled\tDone\tTotal"%"Site"
for key,val in SitesDict.items():
    txt="%12s\t%s\t%s\t%s\t%s\t%s\t%s"%\
          (key[4:], val['Waiting'], val['Running'], val['Failed'], val['Stalled'], val['Done'], val['Total'])
    if float(val['Done'])>0.:
        # More than 10% crash, print bold red
        if float(val['Failed'])/float(val['Done'])>0.1:
            txt=highlight(txt)
    print(txt)

print("%12s\t%s\t%s\t%s\t%s\t%s\t%s"%
          ('Total', StatusDict['Waiting'], StatusDict['Running'], StatusDict['Failed'], StatusDict['Stalled'],
            StatusDict['Done'], StatusDict['Total']))


# print failed
SaveFailed=False
if SaveFailed:
  txt=''
  for job in jobsList:
    majstatus=status['Value'][int(job)]['Status']
    if majstatus=="Failed":
        txt+=str(dirac.getJobSummary(int(job)))+'\n'
  open('failed.txt','w').write(txt)


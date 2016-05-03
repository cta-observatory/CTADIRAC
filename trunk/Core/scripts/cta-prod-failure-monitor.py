#!/bin/env python
"""
  Simple terminal job error summary
  
"""

owner=""
jobGroup=""
nHours=24

def highlight(string):
    return '\x1b[31;1m%s\x1b[0m' % (string)
    
import os, copy
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
Script.gLogger.notice(now)

results=dirac.selectJobs(jobGroup=jobGroup, owner=owner, date=now-nHours*onehour)
if not results.has_key('Value'):
    Script.gLogger.notice("No job found for group \"%s\" and owner \"%s\" in the past %s hours"%
       (jobGroup, owner, nHours))
    Script.sys.exit(0)

# Found some jobs, print information
jobsList=results['Value']
Script.gLogger.notice("%s jobs found for group \"%s\" and owner \"%s\" in the past %s hours\n"%
       (len(jobsList), jobGroup, owner, nHours))

status=dirac.status(jobsList)

# for details
#print dirac.getJobSummary(3075536)

# print out my favourite tables
SitesDict={}

for job in jobsList:
#    print job, status['Value'][int(job)]
    site=status['Value'][int(job)]['Site']
#    site=status['Value'][int(job)]['CE']
    minstatus=status['Value'][int(job)]['MinorStatus']
    majstatus=status['Value'][int(job)]['Status']

    if majstatus not in {'Done', 'Failed'}:
	continue

    if site not in SitesDict.keys():
        if site.find('.')==-1:
	    site='    None' # note that blank spaces are needed
	SitesDict[site] = {'Total':0,'Failed':0,'Errors':{}}

    SitesDict[site]['Total']+=1
    if majstatus=='Failed':
	SitesDict[site]['Failed']+=1
	if minstatus not in SitesDict[site]['Errors'].keys():
	    SitesDict[site]['Errors'][minstatus]=0
	SitesDict[site]['Errors'][minstatus]+=1

Script.gLogger.notice("%20s  Finish  Errors  Rate  Failure reason"%"Site")
for site,val in sorted(SitesDict.items()):
    errstr=""
    for error,amount in val['Errors'].items():
        if len(errstr) > 0:
	    errstr+="\n\t\t\t\t\t    "
	errstr+="%s (%d)" % (error, amount)
    
    txt="%20s%8d%8d%5d%%  %s" % (site, val['Total'], val['Failed'], val['Failed']*100/val['Total'], errstr)
    Script.gLogger.notice(txt)


# print failed
SaveFailed=False
if SaveFailed:
  txt=''
  for job in jobsList:
    majstatus=status['Value'][int(job)]['Status']
    if majstatus=="Failed":
        txt+=str(dirac.getJobSummary(int(job)))+'\n'
  open('failed.txt','w').write(txt)


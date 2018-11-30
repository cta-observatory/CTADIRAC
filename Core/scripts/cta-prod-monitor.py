#!/bin/env python
"""
  Simple terminal job monitoring

"""
import os
import copy
import datetime
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Time import toString, date, day

from CTADIRAC.Core.Utilities import tool_box
from CTADIRAC.Core.Utilities.tool_box import BASE_STATUS_DIR, highlight


Script.setUsageMessage(
    '\n'.join(
        [
            __doc__.split('\n')[1],
            'Usage:',
            '  %s [options]' %
            Script.scriptName,
            'e.g.:',
            '  %s --owner=bregeon --hours=24' %
            Script.scriptName]))

Script.registerSwitch("", "owner=", "the job owner")
Script.registerSwitch("", "jobGroup=", "the job group")
Script.registerSwitch("", "hours=", "Get status for jobs of the last n hours")
Script.registerSwitch("", "failed=", "1 or 0 : Save or not failed jobs in \"failed.txt\"")
Script.parseCommandLine(ignoreErrors=True)


###################
# Start here
if __name__ == '__main__':
    ''' Do something
    '''
    # defaults
    owner = "arrabito"
    job_group = ""
    n_hours = 24
    # arguments
    args = Script.getPositionalArgs()
    for switch in Script.getUnprocessedSwitches():
        if switch[0].lower() == "owner":
            owner = switch[1]
        elif switch[0].lower() == "jobgroup":
            job_group = switch[1]
        elif switch[0].lower() == "hours":
            n_hours = int(switch[1])
        elif switch[0].lower() == "failed":
            SaveFailed = int(switch[1])

    # not at the top !
    from DIRAC.Interfaces.API.Dirac import Dirac
    dirac = Dirac()

    # do the jobs via the 2 main methods
    jobs_list = tool_box.get_job_list(owner, job_group, n_hours)
    Script.gLogger.notice(
        "%s jobs found for group \"%s\" and owner \"%s\" in the past %s hours\n" %
        (len(jobs_list), job_group, owner, n_hours))

    # get status dictionary
    status_dict, sites_dict = tool_box.parse_jobs_list(jobs_list)

    # print out my favourite tables
    Script.gLogger.notice(
        "%16s\tWaiting\tRunning\tFailed\tStalled\tDone\tTotal" %
        "Site")
    for key, val in sites_dict.items():
        txt = "%16s\t%s\t%s\t%s\t%s\t%s\t%s" %\
            (key.split('LCG.')[-1], val['Waiting'], val['Running'], val['Failed'],
             val['Stalled'], val['Done'], val['Total'])
        if float(val['Done']) > 0.:
            # More than 10% crash, print bold red
            if float(val['Failed']) / float(val['Done']) > 0.1:
                txt = highlight(txt)
        Script.gLogger.notice(txt)

    Script.gLogger.notice("%16s\t%s\t%s\t%s\t%s\t%s\t%s" %
                          ('Total', status_dict['Waiting'], status_dict['Running'],
                           status_dict['Failed'], status_dict['Stalled'],
                           status_dict['Done'], status_dict['Total']))

    # print failed
    SaveFailed = False
    if SaveFailed:
        txt = ''
        for job in jobs_list:
            majstatus = status['Value'][int(job)]['Status']
            if majstatus == "Failed":
                txt += str(dirac.getJobSummary(int(job))) + '\n'
        open('failed.txt', 'w').write(txt)

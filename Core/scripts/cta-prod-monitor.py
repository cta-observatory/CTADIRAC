#!/bin/env python
"""
  Simple terminal job monitoring

"""
import os
import copy
import datetime
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Time import toString, date, day
from CTADIRAC.Core.Utilities.tool_box import BASE_STATUS_DIR


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


def highlight(string):
    ''' highlight a string in a terminal display
    '''
    return '\x1b[31;1m%s\x1b[0m' % (string)

def get_job_list(owner, job_group, n_hours):
    ''' get a list of jobs for a selection
    '''
    now = datetime.datetime.now()
    onehour = datetime.timedelta(hours=1)
    results = dirac.selectJobs(jobGroup=job_group,
                               owner=owner,
                               date=now - n_hours * onehour)
    if 'Value' not in results:
        Script.gLogger.notice(
            "No job found for group \"%s\" and owner \"%s\" in the past %s hours" %
            (job_group, owner, n_hours))
        Script.sys.exit(0)

    # Found some jobs, print information)
    jobs_list = results['Value']
    Script.gLogger.notice(
        "%s jobs found for group \"%s\" and owner \"%s\" in the past %s hours\n" %
        (len(jobs_list), job_group, owner, n_hours))
    return jobs_list

def parse_jobs_list(jobs_list):
    ''' parse a jobs list by first getting the status of all jobs
    '''
    # status of all jobs
    status = dirac.status(jobs_list)
    # parse it
    sites_dict = {}
    status_dict = copy.copy(BASE_STATUS_DIR)
    for job in jobs_list:
        site = status['Value'][int(job)]['Site']
        minstatus = status['Value'][int(job)]['MinorStatus']
        majstatus = status['Value'][int(job)]['Status']
        if majstatus not in status_dict.keys():
            Script.gLogger.notice('Add %s to BASE_STATUS_DIR' % majstatus)
            Script.sys.exit(1)
        status_dict[majstatus] += 1
        status_dict['Total'] += 1
        if site not in sites_dict.keys():
            if site.find('.') == -1:
                site = '    None'  # note that blank spaces are needed
            sites_dict[site] = copy.copy(BASE_STATUS_DIR)
            sites_dict[site][majstatus] = 1
            sites_dict[site]["Total"] = 1
        else:
            sites_dict[site]["Total"] += 1
            if majstatus not in sites_dict[site].keys():
                sites_dict[site][majstatus] = 1
            else:
                sites_dict[site][majstatus] += 1
    return status_dict, sites_dict


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
    jobs_list = get_job_list(owner, job_group, n_hours)

    # get status dictionary
    status_dict, sites_dict = parse_jobs_list(jobs_list)

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

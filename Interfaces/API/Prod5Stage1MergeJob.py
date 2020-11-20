"""
  Simple Wrapper on the Job class to run ctapipe stage 1 merging tool

  Nov 19th 2020 - J. Bregeon
                  bregeon@in2p3.fr
"""

__RCSID__ = "$Id$"

# generic imports
import json
import collections

# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Interfaces.API.Prod5Stage1Job import Prod5Stage1Job


class Prod5Stage1MergeJob(Prod5Stage1Job):
    """ Job extension class for ctapipe stage1 processing,
    """
    def __init__(self, cpuTime=432000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Prod5Stage1Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('ctapipe_stage1')
        self.prog_name = 'ctapipe-stage1-merge'
        self.group_size = 5

    def set_meta_data(self, dl1_md):
        """ Set DL1 meta data

        Parameters:
        dl1_md -- metadata dictionary from the telescope simulation
        """
        # # Set evndisp directory meta data
        self.metadata['array_layout'] = dl1_md['array_layout']
        self.metadata['site'] = dl1_md['site']
        self.metadata['particle'] = dl1_md['particle']
        try:
            phiP = dl1_md['phiP']['=']
        except:
            phiP = dl1_md['phiP']
        self.metadata['phiP'] = phiP
        try:
            thetaP = dl1_md['thetaP']['=']
        except:
            thetaP = dl1_md['thetaP']
        self.metadata['thetaP'] = thetaP
        self.metadata[self.program_category+'_prog'] = self.prog_name
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called.
        """
        i_step = 0
        # step 1 -- debug
        if debug:
            ls_step = self.setExecutable( '/bin/ls -alhtr', logFile='LS_Init_Log.txt' )
            ls_step['Value']['name']='Step%i_LS_Init'%i_step
            ls_step['Value']['descr_short']='list files in working directory'
            i_step += 1

            env_step = self.setExecutable( '/bin/env', logFile='Env_Log.txt' )
            env_step['Value']['name']='Step%i_Env'%i_step
            env_step['Value']['descr_short']='Dump environment'
            i_step += 1

        # step 2
        sw_step = self.setExecutable( 'cta-prod-setup-software',
                                  arguments='-p %s -v %s -a stage1 -g %s'%
                                  (self.package, self.version, self.compiler),\
                                  logFile='SetupSoftware_Log.txt')
        sw_step['Value']['name'] = 'Step%i_SetupSoftware' % i_step
        sw_step['Value']['descr_short'] = 'Setup software'
        i_step+=1

        # step 3 verify input data size
        # arguments are nbFiles=0 (not used) and fileSize=1000kB
        eiv_step = self.setExecutable('cta-prod3-verifysteps',
                   arguments="generic %d 10000 '*.h5'" % self.group_size,
                   logFile='Verify_DL1_Inputs_Log.txt')

        eiv_step['Value']['name'] = 'Step%i Verify DL1 input files' % i_step
        eiv_step['Value']['descr_short'] = 'Verify DL1 Inputs'
        i_step += 1

        # step 4 run EventDisplay
        ev_step = self.setExecutable('./dirac_run_stage1_merge',
                                    logFile='ctapipe_stage1_merge_Log.txt')
        ev_step['Value']['name'] = 'Step%i_ctapipe_stage1_merge' % i_step
        ev_step['Value']['descr_short'] = 'Run ctapipe stage 1 merge'
        i_step += 1

        # step 5 set meta data and register Data
        meta_data_json = json.dumps(self.metadata)
        file_meta_data_json = json.dumps(self.file_meta_data)

        meta_data_field = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)',
                           'particle':'VARCHAR(128)',
                           'phiP':'float', 'thetaP': 'float',
                           self.program_category+'_prog':'VARCHAR(128)',
		                   self.program_category+'_prog_version':'VARCHAR(128)',
                           'data_level': 'int', 'configuration_id': 'int'}
        meta_data_field_json = json.dumps(meta_data_field)

        # register Data
        data_output_pattern = './Data/*.h5' # %self.output_data_level
        scripts = '../CTADIRAC/Core/scripts/'
        dm_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                     arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Data" %
                                     (meta_data_json, meta_data_field_json,
                                      file_meta_data_json,
                                      self.base_path, data_output_pattern, self.package,
                                      self.program_category, self.catalogs),
                                     logFile='DataManagement_Log.txt')
        dm_step['Value']['name'] = 'Step%s_DataManagement' % i_step
        dm_step['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
        i_step += 1

        # step 6 register Log
        # log_file_pattern = './*.logs.tgz'
        # file_meta_data = {}
        # file_meta_data_json = json.dumps(file_meta_data)
        # scripts = '../CTADIRAC/Core/scripts/'
        # log_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
        #                               arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Log" %
        #                               (meta_data_json, meta_data_field_json,
        #                                file_meta_data_json,
        #                                self.base_path, log_file_pattern, self.package,
        #                                self.program_category, self.catalogs),
        #                               logFile='LogManagement_Log.txt')
        # log_step['Value']['name'] = 'Step%s_LogManagement' % i_step
        # log_step['Value']['descr_short'] = 'Save log to SE and register them in DFC'
        # i_step += 1

        # step 7 failover step
        failover_step = self.setExecutable('/bin/ls -l',
                                           modulesList=['Script', 'FailoverRequest'])
        failover_step['Value']['name'] = 'Step%s_Failover' % i_step
        failover_step['Value']['descr_short'] = 'Tag files as unused if job failed'
        i_step += 1

        # Step 8 - debug only
        if debug:
            ls_step = self.setExecutable('/bin/ls -Ralhtr', logFile='LS_End_Log.txt')
            ls_step['Value']['name'] = 'Step%s_LSHOME_End' % i_step
            ls_step['Value']['descr_short'] = 'list files in Home directory'
            i_step += 1

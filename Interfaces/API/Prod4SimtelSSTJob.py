"""
  Wrapper on the Job class to handle Simtel Analysis of the PROD4 SST Corsika
  showers

  https://forge.in2p3.fr/issues/35179
                        October 17th 2018 - J. Bregeon
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import DATA_LEVEL_METADATA_ID


class Prod4SimtelSSTJob(Job):
    """ Job extension class for Simtel Analysis

    """
    def __init__(self, cpuTime=432000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('Simtel')
        self.package='corsika_simhessarray'
        self.program_category = 'tel_sim'
        self.version = '2018-09-19'
        self.configuration_id = 4
        self.output_data_level = DATA_LEVEL_METADATA_ID['DL0']
        self.N_output_files = 4
        self.base_path = '/vo.cta.in2p3.fr/MC/PROD4/'
        self.fcc = FileCatalogClient()
        self.metadata = collections.OrderedDict()
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.ts_task_id = 0

    def set_meta_data(self, corsika_md):
        """ Set simtel meta data

        Parameters:
        corsika_md -- metadata dictionary of Corsika files
        """
        # # Set evndisp directory meta data
        self.metadata['array_layout'] = corsika_md['array_layout']
        self.metadata['site'] = corsika_md['site']
        self.metadata['particle'] = corsika_md['particle']
        try:
            phiP = corsika_md['phiP']['=']
        except:
            phiP = corsika_md['phiP']
        # invert phi from Corsika to Simtel as the reference systems is inverted
        self.metadata['phiP'] = str((float(phiP)+180)%360)
        try:
            thetaP = corsika_md['thetaP']['=']
        except:
            thetaP = corsika_md['thetaP']
        self.metadata['thetaP'] = thetaP
        self.metadata[self.program_category+'_prog'] = 'simtel'
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called.
        """
        # step 1 -- debug
        i_step = 1
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_Init'%i_step
            lsStep['Value']['descr_short'] = 'list files in working directory'
            i_step+=1

        # step 2
        swStep = self.setExecutable('cta-prod3-setupsw',
                                  arguments='%s %s'% (self.package,
                                                      self.version),
                                  logFile='SetupSoftware_Log.txt')
        swStep['Value']['name'] = 'Step%i_SetupSoftware' % i_step
        swStep['Value']['descr_short'] = 'Setup software'
        i_step+=1

        # step 3 - running
        evStep = self.setExecutable('./dirac_prod4_sst_simtel',
                                    arguments = "--taskid %s" % (self.ts_task_id),
                                    logFile='Simtel_Log.txt')
        evStep['Value']['name'] = 'Step%i_Simtel' % i_step
        evStep['Value']['descr_short'] = 'Run Simtel'
        i_step += 1

        # step 4 verify merged data
        mgvStep = self.setExecutable( 'cta-prod3-verifysteps', \
                                  arguments = "generic %0d 1000 './*_data.tar'"%\
                                              (self.N_output_files),\
                                  logFile='Verify_Simtel_Log.txt')
        mgvStep['Value']['name']='Step%i_VerifySimtel'%i_step
        mgvStep['Value']['descr_short'] = 'Verify simtel files'
        i_step += 1

        # Step 5 - define meta data, upload file on SE and register in catalogs
        md_json = json.dumps(self.metadata)
        meta_data_field = {'array_layout': 'VARCHAR(128)', 'site': 'VARCHAR(128)',
                           'particle': 'VARCHAR(128)',
                           'phiP': 'float', 'thetaP': 'float',
                           self.program_category + '_prog': 'VARCHAR(128)',
                           self.program_category + '_prog_version': 'VARCHAR(128)',
                           'data_level': 'int', 'configuration_id': 'int'}
        md_field_json = json.dumps(meta_data_field)

        # Upload and register data
        tel_config_list = 'sst-1m sst-astri sst-astri+chec-s sst-gct'.split()
        for config in tel_config_list:
            tel_config = 'cta-prod4-%s' % config
            file_meta_data = {'tel_config' : tel_config}
            file_md_json = json.dumps(file_meta_data)
            output_pattern = './*%s*_data.tar' % tel_config
            scripts = '../CTADIRAC/Core/scripts/'
            dm_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                         arguments="'%s' '%s' '%s' %s %s %s %s '%s' Data" %
                                         (md_json, md_field_json, file_md_json,
                                          self.base_path, output_pattern, self.package,
                                          self.program_category, self.catalogs),
                                         logFile='DataManagement_Log_%s.txt' % tel_config)
            i_step += 1

        # Upload and register log file (contains histograms)
        file_meta_data = {}
        file_md_json = json.dumps(file_meta_data)
        log_file_pattern = './*_log.tar'
        scripts = '../CTADIRAC/Core/scripts/'
        log_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                      arguments="'%s' '%s' '%s' %s %s %s %s '%s' Log" %
                                      (md_json, md_field_json, file_md_json,
                                       self.base_path, log_file_pattern, self.package,
                                       self.program_category, self.catalogs),
                                      logFile='LogManagement_Log.txt')
        log_step['Value']['name'] = 'Step%s_LogManagement' % i_step
        log_step['Value']['descr_short'] = 'Save log to SE and register them in DFC'
        i_step += 1

        # step 5 - debug only
        if debug:
            lsStep=self.setExecutable('/bin/ls -alhtr',logFile='LS_End_Log.txt')
            lsStep['Value']['name']='Step%i_LS_End'%i_step
            lsStep['Value']['descr_short']='list files in working directory and sub-directory'
            i_step += 1

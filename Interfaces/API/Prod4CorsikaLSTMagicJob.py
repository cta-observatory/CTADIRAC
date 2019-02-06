"""
  Job wrapper class to handle Prod4 MC
  Corsika simulations of the LST+MAGIC array
			JB Februrary 2019
"""

__RCSID__ = "$Id$"

# generic imports
import json

# DIRAC imports
import DIRAC
from CTADIRAC.Interfaces.API.Prod4CorsikaSSTJob import Prod4CorsikaSSTJob


class Prod4CorsikaLSTMagicJob(Prod4CorsikaSSTJob):
    """ Job extension class for Prod4 MC simulations,
      takes care of running corsika for the LST+MAGIC positions
      Inherits from Prod4CorsikaSSTJob
      @todo should have a Prod4CorsikaJob really
    """
    def __init__(self, cpu_time=129600):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Prod4CorsikaSSTJob.__init__(self)
        self.setCPUTime(cpu_time)
        self.version = '2018-11-07'
        self.configuration_id = 5
        self.cta_site = 'LaPalma'
        self.output_pattern = 'Data/corsika/run*/*corsika.zst'


    def set_meta_data(self):
        """ define the common meta data of the application
        """
        # The order of the metadata dictionary is important,
        # since it's used to build the directory structure
        self.metadata['array_layout'] = 'Baseline-LSTMagic'
        self.metadata['site'] = self.cta_site
        self.metadata['particle'] = self.particle
        # air shower simulation means North=0 and South=180
        if self.pointing_dir == 'North':
            self.metadata['phiP'] = 0
        if self.pointing_dir == 'South':
            self.metadata['phiP'] = 180
        self.metadata['thetaP'] = float(self.zenith_angle)
        self.metadata[self.program_category + '_prog'] = self.prog_name
        self.metadata[self.program_category + '_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called.
        """
        # Step 1 - debug only
        i_step = 1

        # Step 2 - setup software
        sw_step = self.setExecutable('cta-prod3-setupsw',
                                     arguments='%s %s' % (self.package, self.version),
                                     logFile='SetupSoftware_Log.txt')
        sw_step['Value']['name'] = 'Step%i_SetupSoftware' % i_step
        sw_step['Value']['descr_short'] = 'Setup software'
        i_step += 1

        if debug:
            ls_step = self.setExecutable('/bin/ls -alhtr', logFile='LS_Init_Log.txt')
            ls_step['Value']['name'] = 'Step%i_LS_Init' % i_step
            ls_step['Value']['descr_short'] = 'list files in working directory'
            i_step += 1

        # Step 3 - run corsika
        if self.cta_site == 'LaPalma':
            prod_script = './dirac_prod4_lst-magic_run'
        else:
            DIRAC.gLogger.error('Site not supported: %s' % self.cta_site)
            DIRAC.gLogger.error('No shell script associated')
            DIRAC.exit(-1)

        cs_step = self.setExecutable(prod_script,
                                     arguments='--without-multipipe --start_run %s \
                                               --run %s %s %s %s %s' %
                                     (self.start_run_number, self.run_number,
                                      self.cta_site, self.particle,
                                      self.pointing_dir, self.zenith_angle),
                                     logFile='Corsika_Log.txt')
        cs_step['Value']['name'] = 'Step%i_Corsika' % i_step
        cs_step['Value']['descr_short'] = 'Run Corsika only'
        i_step += 1

        # Step 4 - verify size of corsika output
        csv_step = self.setExecutable('cta-prod3-verifysteps',
                                      arguments='generic %d %d %s' %
                                      (self.n_output_files, self.output_file_size,
                                       self.output_pattern),
                                      logFile='Verify_Corsika_Log.txt')
        csv_step['Value']['name'] = 'Step%i_VerifyCorsika' % i_step
        csv_step['Value']['descr_short'] = 'Verify the Corsika run'
        i_step += 1

        # Step 5 - define meta data, upload file on SE and register in catalogs
        self.set_meta_data()
        md_json = json.dumps(self.metadata)

        meta_data_field = {'array_layout': 'VARCHAR(128)', 'site': 'VARCHAR(128)',
                           'particle': 'VARCHAR(128)',
                           'phiP': 'float', 'thetaP': 'float',
                           self.program_category + '_prog': 'VARCHAR(128)',
                           self.program_category + '_prog_version': 'VARCHAR(128)',
                           'data_level': 'int', 'configuration_id': 'int'}
        md_field_json = json.dumps(meta_data_field)

        # Upload and register data
        file_meta_data = {}
        file_md_json = json.dumps(file_meta_data)

        scripts = '../CTADIRAC/Core/scripts/'
        dm_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                     arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Data" %
                                     (md_json, md_field_json, file_md_json,
                                      self.base_path, self.output_pattern, self.package,
                                      self.program_category, self.catalogs),
                                     logFile='DataManagement_Log.txt')
        dm_step['Value']['name'] = 'Step%s_DataManagement' % i_step
        dm_step['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
        i_step += 1

        # Upload and register log file
        file_meta_data = {}
        file_md_json = json.dumps(file_meta_data)
        log_file_pattern = 'Data/corsika/run*/run*.log'
        scripts = '../CTADIRAC/Core/scripts/'
        log_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                      arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Log" %
                                      (md_json, md_field_json, file_md_json,
                                       self.base_path, log_file_pattern, self.package,
                                       self.program_category, self.catalogs),
                                      logFile='LogManagement_Log.txt')
        log_step['Value']['name'] = 'Step%s_LogManagement' % i_step
        log_step['Value']['descr_short'] = 'Save log to SE and register them in DFC'
        i_step += 1

        # Step 6 - debug only
        if debug:
            ls_step = self.setExecutable('/bin/ls -alhtr', logFile='LS_End_Log.txt')
            ls_step['Value']['name'] = 'Step%s_LSHOME_End' % i_step
            ls_step['Value']['descr_short'] = 'list files in Home directory'
            i_step += 1

        # Number of showers is passed via an environment variable
        self.setExecutionEnv({'NSHOW': '%s' % self.n_shower})

"""
  Job wrapper class to handle Prod4 MC
  Corsika simulations of the SSTs
			JB September 2018
"""

__RCSID__ = "$Id$"

# generic imports
import json
import collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job

class Prod4CorsikaSSTJob(Job) :
    """ Job extension class for Prod4 MC simulations,
      takes care of running corsika for the SST only template
    """
    def __init__(self, cpu_time=432000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.set_cpu_time(cpu_time)
        self.set_name('Prod4_MC_Generation')
        self.package = 'corsika_simhessarray'
        self.program_category = 'airshower_sim'
        self.prog_name = 'corsika'
        self.version = '2018-09-19'
        self.configuration_id = 4
        self.output_data_level = 0
        self.n_shower = 100
        self.start_run_number = '0'
        self.run_number = '10'
        self.cta_site = 'Paranal'
        self.particle = 'gamma'
        self.pointing_dir = 'South'
        self.zenith_angle = 20.
        self.output_pattern = 'Data/corsika/run*/*corsika.zst'
        self.n_output_files = 1
        self.output_file_size = 1000 # kb
        self.base_path = '/vo.cta.in2p3.fr/MC/PROD4/'
        self.catalogs = json.dumps(['DIRACFileCatalog','TSCatalog'])

    def set_run_number(self, runNb):
        """ Set the corsika run number, passed as a string
            because may be a TS parameter

        Parameters:
        runNb -- run number as a string, used as a corsika seed
        """
        self.run_number=runNb

    def set_start_run_number(self, startrunNb):
        """ Set the corsika start run number (to be added to the run_number), passed as a string
            because may be a TS parameter

        Parameters:
        startrunNb -- to be added to the run number
        """
        self.start_run_number = startrunNb

    def set_site(self, site):
        """ Set the site to simulate

        Parameters:
        site -- a string for the site name (LaPalma)
        """
        if site in ['Paranal','LaPalma']:
            DIRAC.gLogger.info ('Set Corsika site to: %s' % site)
            self.site = site
        else:
            DIRAC.gLogger.error ('Site is unknown: %s' % particle)
            DIRAC.exit(-1)

    def set_particle(self, particle):
        """ Set the corsika primary particle

        Parameters:
        particle -- a string for the particle type/name
        """
        if particle in ['gamma','gamma-diffuse','electron','proton','helium']:
            DIRAC.gLogger.info ('Set Corsika particle to: %s' % particle)
            self.particle = particle
        else:
            DIRAC.gLogger.error ('Corsika does not know particle type: %s' % particle)
            DIRAC.exit(-1)

    def set_pointing_dir(self, pointing):
        """ Set the pointing direction, North or South

        Parameters:
        pointing -- a string for the pointing direction
        """
        if pointing in ['North', 'South', 'East', 'West']:
            DIRAC.gLogger.info ('Set Pointing dir to: %s' % pointing)
            self.pointing_dir = pointing
        else:
            DIRAC.gLogger.error ('Unknown pointing direction: %s' % pointing)
            DIRAC.exit(-1)

    def set_meta_data(self):
        """ define the common meta data of the application
        """
        # The order of the metadata dictionary is important,
        # since it's used to build the directory structure
        metadata = collections.OrderedDict()
        metadata['array_layout'] = 'Baseline-SST-only'
        metadata['site'] = self.cta_site
        metadata['particle'] = self.particle
        # air shower simulation means North=0 and South=180
        if self.pointing_dir == 'North':
          metadata['phiP'] = 0
        if self.pointing_dir == 'South':
          metadata['phiP'] = 180
        metadata['thetaP'] = float(self.zenith_angle)
        self.metadata[self.program_category+'_prog'] = self.prog_name
        self.metadata[self.program_category+'_prog_version'] = self.version
        metadata['data_level'] = self.output_data_level
        metadata['configuration_id'] = self.configuration_id

  def setup_work_flow(self, debug=False):
    """ Setup job workflow by defining the sequence of all executables
        All parameters shall have been defined before that method is called.
    """
    # Step 1 - debug only
    i_step = 1
    if debug:
        ls_step = self.setExecutable('/bin/ls -alhtr', logFile = 'LS_Init_Log.txt')
        ls_step['Value']['name']='Step%i_LS_Init' % i_step
        ls_step['Value']['descr_short']='list files in working directory'
        i_step+=1

    # Step 2 - setup software
    sw_step = self.setExecutable('$DIRACROOT/scripts/cta-prod3-setupsw',
                              arguments='%s %s' % (self.package, self.version),
                              logFile='SetupSoftware_Log.txt')
    sw_step['Value']['name'] = 'Step%i_SetupSoftware' % i_step
    sw_step['Value']['descr_short'] = 'Setup software'
    i_step+=1

    # Step 3 - run corsika
    if self.cta_site == 'Paranal':
        prod_script='./dirac_prod4_sst-only_run'
    else:
        DIRAC.gLogger.error('Site not supported: %s' % self.cta_site)
        DIRAC.gLogger.error('No shell script associated')
        DIRAC.exit(-1)

    cs_step = self.setExecutable(prod_script,
                              arguments = '--without-multipipe --start_run %s \
                                           --run %s %s %s %s %s' %
                                         (self.start_run_number, self.run_number,
                                          self.cta_site, self.particle,
                                          self.pointing_dir, self.zenith_angle),
                              logFile='Corsika_Log.txt')
    cs_step['Value']['name']='Step%i_Corsika'%i_step
    cs_step['Value']['descr_short']='Run Corsika only'
    i_step+=1

    # Step 4 - verify size of corsika output
    csv_step = self.setExecutable('$DIRACROOT/scripts/cta-prod3-verify_steps',
                              arguments='generic %d %d %s' %
                              (self.n_output_files, self.output_file_size,
                               self.output_pattern),
                              logFile='Verify_Corsika_Log.txt')
    csv_step['Value']['name']='Step%i_VerifyCorsika'%i_step
    csv_step['Value']['descr_short']='Verify the Corsika run'
    i_step += 1

    # Step 5 - define meta data, upload file on SE and register in catalogs
    self.set_meta_data()
    md_json = json.dumps(self.metadata)

    meta_data_field = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)',
                       'particle':'VARCHAR(128)',
                       'phiP':'float', 'thetaP': 'float',
                       self.program_category+'_prog':'VARCHAR(128)',
	                   self.program_category+'_prog_version':'VARCHAR(128)',
                       'data_level': 'int', 'configuration_id': 'int'}
    md_field_json = json.dumps(meta_data_field)

    file_meta_data = {'runNumber': self.run_number}
    file_md_json = json.dumps(file_meta_data)

    # scripts = '../CTADIRAC/Core/scripts'
    dm_step = self.setExecutable('$DIRACROOT/CTADIRAC/Core/scripts/cta-prod4-managedata.py',
                                arguments="'%s' '%s' '%s' %s %s %s '%s'" %
                                (md_json, md_field_json, file_md_json,
                                 self.output_pattern, self.base_path,
                                 self.start_run_number, self.catalogs),
                                logFile='DataManagement_Log.txt')
    dm_step['Value']['name'] = 'Step%i_DataManagement' % i_step
    dm_step['Value']['descr_short'] = 'Save files to SE and register them in DFC'
    i_step += 1

    # Step 6 - debug only
    if debug:
        ls_step = self.setExecutable('/bin/ls -alhtr',
                                  logFile='LS_End_Log.txt')
        ls_step['Value']['name'] = 'Step%i_LSHOME_End'% i _step
        ls_step['Value']['descr_short'] = 'list files in Home directory'
        i_step += 1

        ls_step2 = self.setExecutable('/bin/ls -alhtr Data/corsika/run*',
                                  logFile='LS_End_Log.txt')
        ls_step['Value']['name'] = 'Step%i_LSDATA_End' % i_step
        ls_step['Value']['descr_short'] = 'list files in Corsika directory'
        i_step += 1

    # Number of showers is passed via an environment variable
    self.setExecutionEnv({'NSHOW' : '%s' % self.n_shower})

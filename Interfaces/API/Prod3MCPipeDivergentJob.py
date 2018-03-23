"""
  Simple Wrapper on the Job class to handle Prod3 MC
  piping corsika into simtels, running Prod3 divergent pointing simulations
			JB,LA March 2018
"""

__RCSID__ = "$Id$"

# generic imports
import json
import collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job


class Prod3MCPipeDivergentJob(Job):
    """ Job extension class for Prod3 MC simulations,
    takes care of running corsika piped into simtel
    one output file is created
    """
    def __init__(self, cpuTime=259200):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('Prod3MC_Generation')
        self.package = 'corsika_simhessarray'
        self.program_category = 'tel_sim'
        self.version = '2017-04-19-div'
        self.configuration_id = 3
        self.output_data_level = 0
        self.nShower = 100
        self.start_run_number = '0'
        self.run_number = '10'
        self.cta_site = 'Paranal'
        self.cta_site_tag = ''
        self.array_layout = 'Baseline'
        self.particle = 'gamma'
        self.pointing_dir = 'South'
        self.zenith_angle = 20.
        self.div_cfg_id = 0
        self.inputpath = 'Data/sim_telarray/cta-prod3-demo/0.0deg'
        self.N_output_files = 1
        self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])

    def setArrayLayout(self, layout):
        """ Set the array layout type

        Parameters:
        layout -- a string for the array layout, hex or square
        """
        if layout in ['Baseline']:
            DIRAC.gLogger.info('Set Simtel layout to: %s' % layout)
            self.array_layout = layout
        else:
            DIRAC.gLogger.error('Unknown layout: : %s' % layout)
            DIRAC.exit(-1)

    def setSite(self, site):
        """ Set the site to simulate

        Parameters:
        site -- a string for the site name (LaPalma)
        """
        self.cta_site = site
        if self.cta_site is not 'LaPalma':
            DIRAC.gLogger.error('Divergent setup available only for LaPalma')
            DIRAC.exit(-1)

    def setParticle(self, particle):
        """ Set the corsika primary particle

        Parameters:
        particle -- a string for the particle type/name
        """
        if particle in ['gamma', 'gamma-diffuse', 'electron',
                        'proton', 'helium']:
            DIRAC.gLogger.info('Set Corsika particle to: %s' % particle)
            self.particle = particle
        else:
            DIRAC.gLogger.error('Corsika does not know particle type: %s'
                                % particle)
            DIRAC.exit(-1)

    def setPointingDir(self, pointing):
        """ Set the pointing direction, North or South

        Parameters:
        pointing -- a string for the pointing direction
        """
        if pointing in ['North', 'South', 'East', 'West']:
            DIRAC.gLogger.info('Set Pointing dir to: %s' % pointing)
            self.pointing_dir = pointing
        else:
            DIRAC.gLogger.error('Unknown pointing direction: %s' % pointing)
            DIRAC.exit(-1)

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called
        """
        # step 1 - debug only
        iStep = 1
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_Init' % iStep
            lsStep['Value']['descr_short'] = 'list files in working directory'
            iStep += 1

        # step 2
        swStep = self.setExecutable('$DIRACROOT/scripts/cta-prod3-setupsw',
                                  arguments='%s %s'% (self.package, self.version),
                                  logFile='SetupSoftware_Log.txt')
        swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
        swStep['Value']['descr_short'] = 'Setup software'
        iStep += 1

        # step 3
        if self.cta_site == 'Paranal':
            prod_script = './dirac_prod3_paranal_divergent'
            DIRAC.gLogger.error('No setup available for Paranal divergent MC yet")
            DIRAC.exit(-1)
        elif self.cta_site == 'LaPalma':
            prod_script = './dirac_prod3_lapalma_divergent'
        else:
            DIRAC.gLogger.error('Unknown site: %s' % self.cta_site)
            DIRAC.gLogger.error('No shell script associated')
            DIRAC.exit(-1)

        csStep = self.setExecutable(prod_script,
                            arguments='--start_run %s --run %s %s %s %s %s' %
                            (self.start_run_number, self.run_number,
                             self.cta_site, self.particle,
                             self.pointing_dir, self.zenith_angle),
                             logFile='CorsikaSimtel_Log.txt')
        csStep['Value']['name'] = 'Step%i_CorsikaSimtel' % iStep
        csStep['Value']['descr_short'] = 'Run Corsika piped into simtel'
        iStep += 1

        # step 4 verify merged data
        mgvStep = self.setExecutable('$DIRACROOT/scripts/cta-prod3-verifysteps', 
                            arguments="generic %0d 1000 '%s/Data/*.simtel.gz'"%
                                      (self.N_output_files, self.inputpath),
                            logFile='Verify_Simtel_Log.txt')
        mgvStep['Value']['name'] = 'Step%i_VerifySimtel' % iStep
        mgvStep['Value']['descr_short'] = 'Verify simtel files'
        iStep += 1

        # step 5 - debug only
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr Data/sim_telarray/*/*/*',
                                        logFile='LS_End_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_End' % iStep
            lsStep['Value']['descr_short'] = 'list files in working directory and sub-directory'
            iStep += 1

        # step 6
        # ## the order of the metadata dictionary is important, since it's used to build the directory structure
        metadata = collections.OrderedDict()
        metadata['array_layout'] = self.array_layout
        metadata['site'] = self.cta_site
        metadata['particle'] = self.particle
        if self.pointing_dir == 'North':
            metadata['phiP'] = 180
        if self.pointing_dir == 'South':
            metadata['phiP'] = 0
        metadata['thetaP'] = float(self.zenith_angle)
        metadata['tel_sim_prog'] = 'simtel'
        metadata['tel_sim_prog_version'] = self.version
        metadata['data_level'] = self.output_data_level
        metadata['configuration_id'] = self.configuration_id
        mdjson = json.dumps(metadata)

        metadatafield = {'array_layout': 'VARCHAR(128)',
                         'site': 'VARCHAR(128)',
                         'particle': 'VARCHAR(128)', 'phiP': 'float',
                         'thetaP': 'float',
                         'tel_sim_prog': 'VARCHAR(128)',
                         'tel_sim_prog_version': 'VARCHAR(128)',
                         'data_level': 'int', 'configuration_id': 'int'}

        mdfieldjson = json.dumps(metadatafield)

        filemetadata = {'runNumber': self.run_number}

        fmdjson = json.dumps(filemetadata)

        dmStep = self.setExecutable('$DIRACROOT/CTADIRAC/Core/scripts/cta-prod3-managedata.py',
                                    arguments="'%s' '%s' '%s' %s %s %s '%s'" %
                                    (mdjson, mdfieldjson, fmdjson,
                                     self.inputpath,
                                     self.basepath, self.start_run_number,
                                     self.catalogs),
                                    logFile='DataManagement_Log.txt')
        dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
        dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
        iStep += 1

        # Number of showers is passed via an environment variable
        self.setExecutionEnv({'NSHOW': '%s' % self.nShower,
                              'DIV_CFG_ID': '%s' % self.div_cfg_id})

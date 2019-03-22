"""
  Wrapper on the Job class to handle a simple ctapipe jobs DL0->DL1->DL2
  following the early pipeline developments by T. Michael
  https://github.com/tino-michael/tino_cta

@authors: J. Bregeon, L. Arrabito, D. Landriu, J. Lefaucheur
            April 2018
"""

__RCSID__ = "$Id$"

# generic imports
import json
import collections
import yaml

# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import DATA_LEVEL_METADATA_ID


class SimpleCtapipeJob(Job):
    """ Job extension class for ctapipe experimental analysis scripts
    in early developments at CEA Saclay DL0->DL2
    Takes care of fine tuning the software installation,
                  classify and reconstruction,
                  merging
    """

    def __init__(self, cpuTime=36000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('ctapipe')
        self.package = 'ctapipe'
        self.program_category = 'calibimgreco'
        self.version = 'v0.5.3'
        self.configuration_id = 1 # To be set according to the input dataset
        # ctapipe params
        self.max_events = '10000000000'
        self.cam_ids = "LSTCam NectarCam"
        ########################################################################
        # data management params
        self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
        self.metadata = collections.OrderedDict()
        self.filemetadata = {}
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])

    def loadConfig(self, config_file):
        """ Load the configuration file using yaml return the configuration dict 

        Parameters:
        config_file -- configuration file for ctapipe
        """
        try:
          with open(name, 'r') as stream:
            cfg = yaml.load(stream)
        except FileNotFoundError as e:
          print e
          raise
        return cfg    
        
    def setConfig(self, config_file):
        """ Set configuration for ctapipe as in the user configuration file

        Parameters:
        config_file -- configuration file for ctapipe
        """
        # Read configuration file
        cfg = self.loadConfig(config_file)

        # Read General section
        self.ana_cfg = cfg['General']['config_file']
        self.modes = cfg['General']['modes']  # One mode now
        self.particle = cfg['General']['particle']
        self.estimate_energy = cfg['General']['estimate_energy']
        self.force_tailcut_for_extended_cleaning = cfg['General']['force_tailcut_for_extended_cleaning']

        if self.force_tailcut_for_extended_cleaning is True:
          self.force_modes = [mode.replace('wave', 'tail') for mode in self.modes]
        else:
          self.force_modes = self.modes

        # Read EnergyRegressor, GammaHadronClassifier, Performance sections
        self.energyRegressor = cfg['EnergyRegressor']
        self.gammaHadronClassifier = cfg['GammaHadronClassifier']
        self.performance = cfg['Performance']

    def getInputfile(self):
        """ Select the input file with the list of LFNs according to the configuration
            and return the selected input file
        """
      input_file = {}
      if self.estimate_energy is False and self.output_type == 'DL1':
        input_file['gamma'] = self.energyRegressor['gamma_list']
      elif self.estimate_energy is True and self.output_type == 'DL1':
        input_file['gamma'] = self.gammaHadronClassifier['gamma_list']
        input_file['proton'] = self.gammaHadronClassifier['proton_list']
      elif self.output_type == 'DL2':
        input_file['gamma'] = self.performance['gamma_list']
        input_file['proton'] = self.performance['proton_list']
        input_file['electron'] = self.performance['electron_list']
      return input_file[self.particle]

    def setCtapipeMD(self, simtelMD):
        """ Set ctapipe meta data

        Parameters:
        simtelMD -- metadata dictionary as for simtel
        """
        # set ctapipe directory meta data
        self.metadata['array_layout'] = simtelMD['array_layout']
        self.metadata['site'] = simtelMD['site']
        self.metadata['particle'] = simtelMD['particle']
        try:
            phiP = simtelMD['phiP']['=']
        except:
            phiP = simtelMD['phiP']
        self.metadata['phiP'] = phiP
        try:
            thetaP = simtelMD['thetaP']['=']
        except:
            thetaP = simtelMD['thetaP']
        self.metadata['thetaP'] = thetaP
        self.metadata[self.program_category+'_prog'] = self.package
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = DATA_LEVEL_METADATA_ID[self.output_type]
        self.metadata['configuration_id'] = self.configuration_id

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called
        """

        # step 1 -- to be removed -- debug only
        iStep = 1
        if debug:
            step = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            step['Value']['name'] = 'Step%i_LS_Init' % iStep
            step['Value']['descr_short'] = 'list files in working directory'
            iStep += 1

        # step 2
        step = self.setExecutable('cta-prod3-setupsw',
                                    arguments='%s %s %s' %
                                    (self.package, self.version, self.program_category),
                                    logFile='SetupSoftware_Log.txt')
        step['Value']['name'] = 'Step%i_SetupSoftware' % iStep
        step['Value']['descr_short'] = 'Setup software'
        iStep += 1

        # step 2bis
        # arguments are nbFiles=0 (not used) and fileSize=100kB
        step = self.setExecutable('cta-prod3-verifysteps',
                                     arguments='analysisinputs 0 100',
                                     logFile='Verify_CtapipeInputs_Log.txt')
        step['Value']['name'] = 'Step%i_VerifyPipeInputs' % iStep
        step['Value']['descr_short'] = 'Verify ctapipe Inputs'
        iStep += 1

        # step 3: run ctapipe write_dl1 or write_dl2
        for mode in self.force_modes:
          if self.output_type == 'DL1':
            ctapipe_exe = './dirac_ctapipe_dl1'
            ctapipe_args = "--config_file %s --estimate_energy %s --max_events %s --mode %s --cam_ids '%s'" % \
                         ( self.ana_cfg, self.estimate_energy, self.max_events, mode, self.cam_ids )
          elif self.output_type == 'DL2':
            ctapipe_exe = './dirac_ctapipe_dl2'
            ctapipe_args = "--config_file %s --max_events %s --mode %s --force_tailcut_for_extended_cleaning %s --cam_ids '%s'" % \
                            ( self.ana_cfg, self.max_events, mode, self.force_tailcut_for_extended_cleaning, self.cam_ids )
            step = self.setExecutable(ctapipe_exe, arguments = ctapipe_args, logFile='Ctapipe_Log.txt')
            step['Value']['name'] = 'Step%i_Ctapipe' % iStep
            step['Value']['descr_short'] = 'Run Ctapipe'
            iStep += 1
            
          # step 4: run merge tables (if only 1 file is found the script exits gracefully)
          ctapipe_exe = './dirac_ctapipe_merge_tables'
          ctapipe_args = "--mode %s" % mode 
          step = self.setExecutable(ctapipe_exe, arguments = ctapipe_args, logFile='MergeTables_Log.txt')
          step['Value']['name'] = 'Step%i_MergeTables' % iStep
          step['Value']['descr_short'] = 'Run Merge tables'
          iStep += 1
          
        
        # step 4
        # ## the order of the metadata dictionary is important, since it's used to build the directory structure
        mdjson = json.dumps(self.metadata)
        metadatafield = {'array_layout': 'VARCHAR(128)',
                         'site': 'VARCHAR(128)',
                         'particle': 'VARCHAR(128)', 'phiP': 'float',
                         'thetaP': 'float',
                         self.program_category+'_prog': 'VARCHAR(128)',
                         self.program_category+'_prog_version': 'VARCHAR(128)',
                         'data_level': 'int', 'configuration_id': 'int'}
        mdfieldjson = json.dumps(metadatafield)

        # register Data
        outputpattern = './*.h5'
        file_md_json = json.dumps(self.filemetadata)
        scripts = '../CTADIRAC/Core/scripts'
        step = self.setExecutable(scripts + '/cta-analysis-managedata.py',
                            arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s'" %\
                            (mdjson, mdfieldjson, file_md_json, self.basepath,
                            outputpattern, self.package, self.program_category,
                            self.catalogs),
                            logFile='DataManagement_Log.txt')
        step['Value']['name'] = 'Step%i_DataManagement' % iStep
        step['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
        iStep += 1
 
        # step 5 -- to be removed -- debug only
        if debug:
            step = self.setExecutable('/bin/ls -alhtr ./ ',
                                        logFile='LS_End_Log.txt')
            step['Value']['name'] = 'Step%i_LS_End' % iStep
            step['Value']['descr_short'] = 'list files in working and Data directory'
            iStep += 1

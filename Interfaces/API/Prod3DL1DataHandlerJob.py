"""
  Wrapper on the Job class to handle dl1_data_handler DL0->"DL1"

  https://forge.in2p3.fr/issues/35992
                        March 6th 2019 - J. Bregeon

"""

__RCSID__ = "$Id$"

# generic imports
import json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import DATA_LEVEL_METADATA_ID


class Prod3DL1DataHandlerJob(Job):
    """ Job extension class for dl1_data_handler DL0->DL1 conversion
      of simtel files to HDF5 conversion for Machine Learning fans
    """

    def __init__(self, cpuTime=36000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('dl1 data handler reduction')
        self.package = 'dl1_data_handler'
        self.program_category = 'calibimgreco'
        self.version = 'v0.7.2'
        self.configuration_id = 1
        self.output_data_level = DATA_LEVEL_METADATA_ID['DL1']
        self.N_output_files = 1
        self.prefix = 'CTA.prod3Sb'
        self.layout = 'Baseline'
        self.base_path = '/vo.cta.in2p3.fr/MC/PROD3/'
        self.fcc = FileCatalogClient()
        self.metadata = collections.OrderedDict()
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.ts_task_id = 0

    def set_meta_data(self, simtel_md):
        """ Set dl1_data_handler meta data

        Parameters:
        simtel_md -- metadata dictionary as for simtel
        """
        self.metadata['array_layout'] = simtel_md['array_layout']
        self.metadata['site'] = simtel_md['site']
        self.metadata['particle'] = simtel_md['particle']
        try:
            phiP = simtel_md['phiP']['=']
        except:
            phiP = simtel_md['phiP']
        self.metadata['phiP'] = phiP
        try:
            thetaP = simtel_md['thetaP']['=']
        except:
            thetaP = simtel_md['thetaP']
        self.metadata['thetaP'] = thetaP
        self.metadata[self.program_category+'_prog'] = self.package
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called
        """

        # step 1 -- debug only
        iStep = 1
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_Init' % iStep
            lsStep['Value']['descr_short'] = 'list files in working directory'
            iStep += 1

        # step 2
        swStep = self.setExecutable('cta-prod3-setupsw',
                                    arguments='%s %s %s' %
                                    (self.package, self.version, self.program_category),
                                    logFile='SetupSoftware_Log.txt')
        swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
        swStep['Value']['descr_short'] = 'Setup software'
        iStep += 1

        # step 3
        # arguments are nbFiles=0 (not used) and fileSize=100kB
        eivStep = self.setExecutable('cta-prod3-verifysteps',
                                     arguments='analysisinputs 0 100',
                                     logFile='Verify_EvnDispInputs_Log.txt')
        eivStep['Value']['name'] = 'Step%i_VerifyEvnDispInputs' % iStep
        eivStep['Value']['descr_short'] = 'Verify EvnDisp Inputs'
        iStep += 1

        # step 4
        evStep = self.setExecutable('./dirac_process_runs',
                                    logFile='dl1_handler_Log.txt')
        evStep['Value']['name'] = 'Step%i_dl1_data_handler' % iStep
        evStep['Value']['descr_short'] = 'Run the DL1 Data Handler'
        iStep += 1

        # Step 5 - define meta data, upload file on SE and register in catalogs
        md_json = json.dumps(self.metadata)
        meta_data_field = {'array_layout': 'VARCHAR(128)', 'site': 'VARCHAR(128)',
                           'particle': 'VARCHAR(128)',
                           'phiP': 'float', 'thetaP': 'float',
                           self.program_category + '_prog': 'VARCHAR(128)',
                           self.program_category + '_prog_version': 'VARCHAR(128)',
                           'data_level': 'int', 'configuration_id': 'int'}
        md_field_json = json.dumps(meta_data_field)

        # register Data
        file_meta_data = {}
        file_md_json = json.dumps(file_meta_data)
        output_pattern = './Data/*.h5'
        scripts = '../CTADIRAC/Core/scripts/'
        dm_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                     arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Data" %
                                     (md_json, md_field_json, file_md_json,
                                      self.base_path, output_pattern, self.package,
                                      self.program_category, self.catalogs),
                                     logFile='DataManagement_Log.txt')
        dm_step['Value']['name'] = 'Step%i_DataManagement' % iStep
        dm_step['Value']['descr_short'] = 'Save files to SE and register them in DFC'
        iStep += 1

        # register Log
        # outputpattern = './*.logs.tgz'
        # filemetadata = {}
        # file_md_json = json.dumps(filemetadata)
        # dmStep = self.setExecutable('../CTADIRAC/Core/scripts/cta-analysis-managedata.py',
        #                           arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s' Log" % \
        #                           (mdjson, mdfieldjson, file_md_json, self.basepath,
        #                            outputpattern, self.package, self.program_category, self.catalogs),
        #                           logFile = 'Log_DataManagement_Log.txt')
        # dmStep['Value']['name'] = 'Step%i_Log_DataManagement' % iStep
        # dmStep['Value']['descr_short'] = 'Save log files to SE and register them in DFC'
        # iStep += 1

        # step 6 -- to be removed -- debug only
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_End_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_End' % iStep
            lsStep['Value']['descr_short'] = 'list files in working and Data directory'
            iStep += 1

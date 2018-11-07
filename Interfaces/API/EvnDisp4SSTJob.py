"""
  Simple Wrapper on the Job class to handle EvnDisp Analysis
  for the Prod4 SST run
  https://forge.in2p3.fr/issues/35197
                        October 31st 2018 - J. Bregeon
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import DATA_LEVEL_METADATA_ID


class EvnDisp4SSTJob(Job):
    """ Job extension class for EvnDisp Analysis,
      takes care of running converter and evndisp.
    """

    def __init__(self, cpuTime=432000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('Evndisplay_CalibReco')
        self.package='evndisplay'
        self.program_category = 'calibimgreco'
        self.version = 'prod4_d20181028'
        self.configuration_id = 4
        self.output_data_level = DATA_LEVEL_METADATA_ID['DL1']
        self.N_output_files = 1
        self.prefix = 'CTA.prod4S'
        self.layout = '3HB9-SST'
        self.calibration_file = 'prod4-SST-IPR.root'
        self.reconstructionparameter = 'EVNDISP.prod4.reconstruction.runparameter.NN.noLL'
        self.base_path = '/vo.cta.in2p3.fr/MC/PROD4/'
        self.fcc = FileCatalogClient()
        self.metadata = collections.OrderedDict()
        self.filemetadata = {}
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.ts_task_id = 0

    def set_meta_data(self, simtel_md):
        """ Set evndisplay meta data

        Parameters:
        simtel_md -- metadata dictionary as for simtel
        """
        # # Set evndisp directory meta data
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
        self.metadata[self.program_category+'_prog'] = 'evndisp'
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def set_file_meta_data(self, simtel_md):
        """ Set evndisplay file meta data

        Parameters:
        simtel_md -- metadata dictionary as for simtel
        """
        # # Set evndisp directory meta data
        self.filemetadata['tel_config'] = simtel_md['tel_config']

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called.
        """
        # step 1 -- debug
        iStep = 1
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_Init'%iStep
            lsStep['Value']['descr_short'] = 'list files in working directory'
            iStep+=1

        # step 2
        swStep = self.setExecutable('cta-prod3-setupsw',
                                  arguments='%s %s'% (self.package,
                                                      self.version),
                                  logFile='SetupSoftware_Log.txt')
        swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
        swStep['Value']['descr_short'] = 'Setup software'
        iStep+=1

        # step 3
        evStep = self.setExecutable('./dirac_prod4_sst_evndisp',
                                    arguments = "--prefix %s --layout '%s' \
                                    --calibration_file %s \
                                    --reconstructionparameter %s  --taskid %s" %
                                    (self.prefix, self.layout,
                                     self.calibration_file,
                                     self.reconstructionparameter, self.ts_task_id),
                                    logFile='EvnDisp_Log.txt')
        evStep['Value']['name'] = 'Step%i_EvnDisplay' % iStep
        evStep['Value']['descr_short'] = 'Run EvnDisplay'
        iStep += 1

        # step 4
        # ## the order of the metadata dictionary is important, since it's used to build the directory structure
        md_json = json.dumps(self.metadata)
        meta_data_field = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)',
                           'particle':'VARCHAR(128)',
                           'phiP':'float', 'thetaP': 'float',
                           self.program_category+'_prog':'VARCHAR(128)',
		                   self.program_category+'_prog_version':'VARCHAR(128)',
                           'data_level': 'int', 'configuration_id': 'int'}
        md_field_json = json.dumps(meta_data_field)

        # register Data
        outputpattern = './*evndisp-*-DL%01d.tar.gz'%self.output_data_level
        file_md_json = json.dumps(self.filemetadata)
        scripts = '../CTADIRAC/Core/scripts/'
        dm_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                     arguments="'%s' '%s' '%s' %s %s %s %s '%s' Data" %
                                     (md_json, md_field_json, file_md_json,
                                      self.base_path, output_pattern, self.package,
                                      self.program_category, self.catalogs),
                                     logFile='DataManagement_%s_Log.txt' % tel_config)
        dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
        dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
        iStep += 1

        # register Log
        log_file_pattern = './*.logs.tgz'
        filemetadata = {}
        file_md_json = json.dumps(filemetadata)
        scripts = '../CTADIRAC/Core/scripts/'
        log_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                      arguments="'%s' '%s' '%s' %s %s %s %s '%s' Log" %
                                      (md_json, md_field_json, file_md_json,
                                       self.base_path, log_file_pattern, self.package,
                                       self.program_category, self.catalogs),
                                      logFile='LogManagement_Log.txt')
        log_step['Value']['name'] = 'Step%i_Log_DataManagement' % iStep
        log_step['Value']['descr_short'] = 'Save log files to SE and register them in DFC'
        iStep += 1

        # step 6 -- to be removed -- debug only
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile = 'LS_End_Log.txt')
            lsStep['Value']['name']='Step%i_LS_End'%iStep
            lsStep['Value']['descr_short']='list files in working directory and in Data directory'
            iStep+=1

"""
  Simple Wrapper on the Job class to handle Simtel Analysis

  https://forge.in2p3.fr/issues/33932
                        June 27th 2018 - J. Bregeon, C. Bigongiari
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class SimtelTSJob(Job):
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
        self.package='corsika_simtelarray'
        self.program_category = 'tel_sim'
        self.version = ''
        self.configuration_id = -1
        self.output_data_level = 0
        self.basepath = '/vo.cta.in2p3.fr/user/c/ciro.bigongiari'
        self.fcc = FileCatalogClient()
        self.metadata = collections.OrderedDict()
        self.filemetadata = {}
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.ts_task_id = 1000

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

        # step 2bis
        # arguments are nbFiles=0 (not used) and fileSize=100kB
        eivStep = self.setExecutable('cta-prod3-verifysteps',
                                  arguments='analysisinputs 0 100',
                                  logFile='Verify_SimtelInputs_Log.txt')
        eivStep['Value']['name'] = 'Step%i_VerifySimtelInputs' % iStep
        eivStep['Value']['descr_short'] = 'Verify Simtel Inputs'
        iStep += 1

        evStep = self.setExecutable('./dirac_simtel_zstd.sh',
                                    arguments = "--prefix %s --layout_list '%s' \
                                    --focal_file %s --calibration_file %s \
                                    --reconstructionparameter %s  --taskid %s" %
                                    (self.prefix, self.layout_list,
                                     self.focal_file, self.calibration_file,
                                     self.reconstructionparameter, self.ts_task_id),
                                    logFile='Simtel_Log.txt')
        evStep['Value']['name'] = 'Step%i_Simtel' % iStep
        evStep['Value']['descr_short'] = 'Run Simtel'
        iStep += 1

        # step 4
        # ## the order of the metadata dictionary is important, since it's used to build the directory structure
        mdjson = json.dumps(self.metadata)

        metadatafield = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)',
                         'particle':'VARCHAR(128)',
                         'phiP':'float', 'thetaP': 'float',
                         self.program_category+'_prog':'VARCHAR(128)',
		                 self.program_category+'_prog_version':'VARCHAR(128)',
                         'data_level': 'int', 'configuration_id': 'int'}
        mdfieldjson = json.dumps(metadatafield)

        # register Data
        outputpattern = './*evndisp-DL%01d.tar.gz'%self.output_data_level
        file_md_json = json.dumps(self.filemetadata)
        scripts = '../CTADIRAC/Core/scripts'
        dmStep = self.setExecutable(scripts + '/cta-analysis-managedata.py',
                            arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s'" %
                            (mdjson, mdfieldjson, file_md_json, self.basepath,
                            outputpattern, self.package,
                            self.program_category, self.catalogs),
                            logFile='DataManagement_Log.txt')
        dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
        dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
        iStep += 1

        # register Log
        outputpattern = './*.logs.tgz'
        filemetadata = {}
        file_md_json = json.dumps(filemetadata)
        dmStep = self.setExecutable('../CTADIRAC/Core/scripts/cta-analysis-managedata.py',
                                  arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s' Log" % \
                                  (mdjson, mdfieldjson, file_md_json, self.basepath,
                                   outputpattern, self.package, self.program_category, self.catalogs),
                                  logFile = 'Log_DataManagement_Log.txt')
        dmStep['Value']['name'] = 'Step%i_Log_DataManagement' % iStep
        dmStep['Value']['descr_short'] = 'Save log files to SE and register them in DFC'
        iStep += 1

        # step 6 -- to be removed -- debug only
        if debug:
            lsStep = self.setExecutable('/bin/ls',
                                        arguments = " -alhtr; /bin/ls -lahrt ./Data",
                                        logFile = 'LS_End_Log.txt')
            lsStep['Value']['name']='Step%i_LS_End'%iStep
            lsStep['Value']['descr_short']='list files in working directory and in Data directory'
            iStep+=1

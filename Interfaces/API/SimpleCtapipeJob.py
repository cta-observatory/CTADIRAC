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
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient


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
        self.package = 'ctapipe_exp'
        self.program_category = 'calibimgreco'
        self.version = 'v0.5.3'
        self.configuration_id = -1
        self.output_data_level = 2
        self.prefix = 'CTA.prod3Nb'
        self.layout = 'Baseline'
        self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
        self.outputpattern = './*.hdf5'
        self.metadata = collections.OrderedDict()
        self.filemetadata = dict()
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])

    def setTSTaskId(self, taskid):
        """ Set TS task Id, dynamically resolved at job run time

        Parameters:
        taskid -- an int
        """
        self.ts_task_id = taskid

    def set_metadata(self, simtelMD):
        """ Set ctaipe meta data from a dictionnary

        Parameters:
        simtelMD -- metadata dictionary as for simtel
        """
        # set directory meta data
        self.metadata['array_layout'] = simtelMD['array_layout']
        self.metadata['site'] = simtelMD['site']
        self.metadata['particle'] = simtelMD['particle']
        self.metadata['phiP'] = simtelMD['phiP']['=']
        self.metadata['thetaP'] = simtelMD['thetaP']['=']
        self.metadata[self.program_category+'_prog'] = self.package
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called
        """

        # step 1 -- to be removed -- debug only
        iStep = 1
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_Init' % iStep
            lsStep['Value']['descr_short'] = 'list files in working directory'
            iStep += 1

        # step 2
        swStep = self.setExecutable('cta-prod3-setupsw',
                                    arguments='%s %s' %
                                    (self.package, self.version),
                                    logFile='SetupSoftware_Log.txt')
        swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
        swStep['Value']['descr_short'] = 'Setup software'
        iStep += 1

        # step 2bis
        # arguments are nbFiles=0 (not used) and fileSize=100kB
        eivStep = self.setExecutable('cta-prod3-verifysteps',
                                     arguments='analysisinputs 0 100',
                                     logFile='Verify_PipeInputs_Log.txt')
        eivStep['Value']['name'] = 'Step%i_VerifyPipeInputs' % iStep
        eivStep['Value']['descr_short'] = 'Verify ctapipe Inputs'
        iStep += 1

        # step 5
        evStep = self.setExecutable('./dirac_process_runs',
                                    logFile='IE_SCT_Log.txt')
        evStep['Value']['name'] = 'Step%i_ImageExtractor' % iStep
        evStep['Value']['descr_short'] = 'Run the Image Extractor'
        iStep += 1

        # step 6
        # ## the order of the metadata dictionary is important,
        # since it's used to build the directory structure
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
#        outputpattern = '*./Data/*DL%01d.hdf5' % self.output_data_level
        outputpattern = './Data/*.h5'
        file_md_json = json.dumps(self.filemetadata)
        scripts = '../CTADIRAC/Core/scripts'
        dmStep = self.setExecutable(scripts + '/cta-analysis-managedata.py',
                            arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s'" %\
                            (mdjson, mdfieldjson, file_md_json, self.basepath,
                            outputpattern, self.package, self.program_category,
                            self.catalogs),
                            logFile='DataManagement_Log.txt')
        dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
        dmStep['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
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
            lsStep = self.setExecutable('/bin/ls -alhtr ./ ',
                                        logFile='LS_End_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_End' % iStep
            lsStep['Value']['descr_short'] = 'list files in working and Data directory'
            iStep += 1

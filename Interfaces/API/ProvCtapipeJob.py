"""
  Wrapper on the Job class to handle a simple ctapipe job using provenance
"""

__RCSID__ = "$Id$"

# generic imports
import json
import collections

# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import DATA_LEVEL_METADATA_ID


class ProvCtapipeJob(Job):
    """ Job extension class for provenance test with ctapipe 
    """

    def __init__(self, cpuTime=36000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('prov_test')
        self.package = 'provtest'
        self.program_category = 'calibimgreco'
        self.version = 'v0.5.3'
        self.configuration_id = 1 # To be set according to the input dataset
        ########################################################################
        # data management params
        self.basepath = '/vo.cta.in2p3.fr/user/a/arrabito'
        self.metadata = collections.OrderedDict()
        self.filemetadata = {}
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])

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

        # step 3: run MuonRecProv_step1 (run the reco algo)
        ctapipe_exe = './dirac_provctapipe'        
        self.setExecutable(ctapipe_exe, logFile='Ctapipe_Log.txt')
        step['Value']['name'] = 'Step%i_Ctapipe' % iStep
        step['Value']['descr_short'] = 'Run Ctapipe'
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
        outputpattern = './*.hdf5'
        file_md_json = json.dumps(self.filemetadata)
        
        scripts = '../CTADIRAC/Core/scripts'
        step = self.setExecutable(scripts + '/cta-prod-managedata.py',
                            arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s' %s" %\
                            (mdjson, mdfieldjson, file_md_json, self.basepath,
                            outputpattern, self.package, self.program_category,
                             self.catalogs,"Data"),
                            logFile='DataManagement_Log.txt')
        step['Value']['name'] = 'Step%i_DataManagement' % iStep
        step['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
        iStep += 1

        # step 5: add Prov Data
        self.setExecutable('cta-prod-add-prov', logFile='AddProvData_Log.txt')
        step['Value']['name'] = 'Step%i_Prov' % iStep
        step['Value']['descr_short'] = 'Run Prov'
        iStep += 1
        
        # step 6 -- to be removed -- debug only
        if debug:
            step = self.setExecutable('/bin/ls -alhtr ./ ',
                                        logFile='LS_End_Log.txt')
            step['Value']['name'] = 'Step%i_LS_End' % iStep
            step['Value']['descr_short'] = 'list files in working and Data directory'
            iStep += 1

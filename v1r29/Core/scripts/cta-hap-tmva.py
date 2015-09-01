#!/usr/bin/env python
import DIRAC
import os

def setVersion( optionValue ):
  global version
  version = optionValue
  return DIRAC.S_OK()

def setAnalysisType( optionValue ):
  global analysistype
  analysistype = optionValue
  return DIRAC.S_OK()

def setCutsConfig( optionValue ):
  global cutsconfig
  cutsconfig = optionValue
  return DIRAC.S_OK()

def setRunlist( optionValue ):
  global runlist
  runlist = optionValue
  return DIRAC.S_OK()

def setArrayConfig( optionValue ):
  global arrayconfig
  arrayconfig = optionValue
  return DIRAC.S_OK()

def setEnergyMethod( optionValue ):
  global energymethod
  energymethod = optionValue
  return DIRAC.S_OK()

def setParticleType( optionValue ):
  global particle
  particle = optionValue
  return DIRAC.S_OK()

def setZenith( optionValue ):
  global zenith
  zenith = optionValue
  return DIRAC.S_OK()

def setOffset( optionValue ):
  global offset
  offset = optionValue
  return DIRAC.S_OK()
  
def sendOutput(stdid,line):
  DIRAC.gLogger.notice(line)

def main():

  from DIRAC.Core.Base import Script
### DoCtaIrf options ##########################################################
  Script.registerSwitch( "A:", "analysis=", "Analysis Type", setAnalysisType )
  Script.registerSwitch( "C:", "cuts=", "Cuts Config", setCutsConfig )
  Script.registerSwitch( "R:", "runlist=", "Runlist", setRunlist )
  Script.registerSwitch( "Z:", "zenith=", "Zenith", setZenith )
  Script.registerSwitch( "O:", "offset=", "Offset", setOffset )
  Script.registerSwitch( "M:", "energy=", "Energy Method", setEnergyMethod )
  Script.registerSwitch( "T:", "arrayconfig=", "Array Configuration", setArrayConfig )
  Script.registerSwitch( "P:", "particle=", "Particle Type", setParticleType )
## other options
  Script.registerSwitch( "V:", "version=", "HAP version", setVersion )

  Script.parseCommandLine( ignoreErrors = True )
  
  args = Script.getPositionalArgs()
  if len( args ) < 1:
    Script.showHelp()

  from CTADIRAC.Core.Workflow.Modules.HapApplication import HapApplication
  from CTADIRAC.Core.Workflow.Modules.HapRootMacro import HapRootMacro
  from CTADIRAC.Core.Utilities.SoftwareInstallation import checkSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import installSoftwarePackage
  from CTADIRAC.Core.Utilities.SoftwareInstallation import getSoftwareEnviron
  from CTADIRAC.Core.Utilities.SoftwareInstallation import localArea
  from CTADIRAC.Core.Utilities.SoftwareInstallation import sharedArea
  from DIRAC.Core.Utilities.Subprocess import systemCall
  from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
  
  jobID = os.environ['JOBID']
  jobID = int( jobID )
  jobReport = JobReport( jobID )

  ha = HapApplication()

  HapPack = 'HAP/' + version + '/HAP'

  packs = ['HESS/v0.2/lib','HESS/v0.3/root',HapPack]

  for package in packs:
    DIRAC.gLogger.notice( 'Checking:', package )
    if sharedArea:
      if checkSoftwarePackage( package, sharedArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Shared Area:', package )
        continue
    if localArea:
      if checkSoftwarePackage( package, localArea() )['OK']:
        DIRAC.gLogger.notice( 'Package found in Local Area:', package )
        continue
      if installSoftwarePackage( package, localArea() )['OK']:
        continue
    DIRAC.gLogger.error( 'Check Failed for software package:', package )
    DIRAC.gLogger.error( 'Software package not available')
    DIRAC.exit( -1 )  

  ha.setSoftwarePackage(HapPack)

  ha.hapExecutable = 'DoCtaIrf'

  runlistdir = os.environ['PWD']

  build_infile(runlist)

  ha.hapArguments = [analysistype, cutsconfig, runlistdir, runlist, zenith, offset, arrayconfig, energymethod, particle]
 
  DIRAC.gLogger.notice( 'Executing Hap Application' )
  res = ha.execute()

  if not res['OK']:
    DIRAC.gLogger.error( 'Failed to execute Hap Application')
    jobReport.setApplicationStatus('Hap Application: Failed')
    DIRAC.exit( -1 )
    
###################### Check TTree Output File #######################    
  outfile = 'MVAFile_' + runlist + ".root"

  if not os.path.isfile(outfile):
    error = 'TTree file was not created:'
    DIRAC.gLogger.error( error, outfile )
    jobReport.setApplicationStatus('DoCtaIrf: TTree file not created')
    DIRAC.exit( -1 )

###################### Quality Check for TTree Output File: step0######################
  hr = HapRootMacro()
  hr.setSoftwarePackage(HapPack)
 
  DIRAC.gLogger.notice('Executing TTree check step0')
  hr.rootMacro = '/hapscripts/mva/Open_TT.C+'
  outfilestr = '"' + outfile + '"'
  args = [outfilestr]
  DIRAC.gLogger.notice( 'Open_TT macro Arguments:', args )
  hr.rootArguments = args
  DIRAC.gLogger.notice( 'Executing Hap Open_TT macro')
  res = hr.execute()

  if not res['OK']:
    DIRAC.gLogger.error( 'Open_TT: Failed' )
    DIRAC.exit( -1 )

#########################Quality Check for TTree Output File: step1####################                                                                                         
  DIRAC.gLogger.notice('Executing TTree check step1')

  ret = getSoftwareEnviron(HapPack)
  if not ret['OK']:
    error = ret['Message']
    DIRAC.gLogger.error( error, HapPack)
    DIRAC.exit( -1 )

  hapEnviron = ret['Value']
  hessroot =  hapEnviron['HESSROOT']

  check_script = hessroot + '/hapscripts/mva/check_TT.csh'

  cmdTuple = [check_script]
  ret = systemCall( 0, cmdTuple, sendOutput)

  if not ret['OK']:
    DIRAC.gLogger.error( 'Failed to execute TTree Check step1')
    jobReport.setApplicationStatus('Check_TTree: Failed')
    DIRAC.exit( -1 )
#############################################
      
  DIRAC.exit()

def build_infile(runlist):

  from DIRAC.Resources.Catalog.PoolXMLCatalog import PoolXMLCatalog

  pm = PoolXMLCatalog('pool_xml_catalog.xml')
  runlist = runlist + '.list'
  f = open( runlist ,'a')

  for Lfn in pm.getLfnsList():
    pfn = pm.getPfnsByLfn(Lfn)['Replicas']['Uknown']
    RunNum = pfn.split( 'dst_CTA_' )[1].split('.root')[0]
    pfn = RunNum  + ' ' + '-1 ' + pfn
    f.write(pfn)
    f.write('\n')

  f.close()
  return DIRAC.S_OK()


if __name__ == '__main__':

  try:
    main()
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit( -1 )



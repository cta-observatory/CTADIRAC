#!/usr/bin/env python

"""
Get directory size
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN directory path (wildcard accepted but limited to the deepest directory)'] ) )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
import os.path 
import fnmatch

exitCode = 0
args = Script.getPositionalArgs()

if len(args)!=1:
  Script.showHelp()

searchdir = args[0]
if args[0][-1] == '/':
  searchdir = args[0][:-1]

path = os.path.abspath(os.path.join(searchdir, os.pardir))
fcc = FileCatalogClient()
res = fcc.listDirectory(path)
subdirs = res['Value']['Successful'][path]['SubDirs'].keys()
filtered = fnmatch.filter(subdirs, searchdir)

if len(filtered) == 0:
  print "No directories matched"
  DIRAC.exit( exitCode )

tot_size = 0.
tot_disk_size = {}
for subdir in filtered:
  res = fcc.getDirectorySize(subdir,True,False)
  size = res['Value']['Successful'][subdir]['LogicalSize']
  print "%s: %.1f TB" % (subdir, size/10.**12)
  tot_size = tot_size + size
  size_dict = res['Value']['Successful'][subdir]['PhysicalSize']
  for disk in size_dict.keys():
    if disk != 'TotalSize' and disk != 'TotalFiles':
      disk_size = res['Value']['Successful'][subdir]['PhysicalSize'][disk]['Size']
      print disk + " size: %.1f TB" % (disk_size/10.**12)
      if tot_disk_size.has_key(disk):
        tot_disk_size[disk] = tot_disk_size[disk] + disk_size
      else:
        tot_disk_size[disk] = disk_size

print "Total size: %.1f TB" % (tot_size/10.**12)

for disk in tot_disk_size.keys():
  print disk + " size: %.1f TB -> %.1f" % ((tot_disk_size[disk]/10.**12),(tot_disk_size[disk]/tot_size)*100) + '%'



DIRAC.exit( exitCode )



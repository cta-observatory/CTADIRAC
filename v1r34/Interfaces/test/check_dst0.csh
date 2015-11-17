#! /bin/csh

set MyTmpDir="."
chmod +rw ${MyTmpDir}

set CHECKFILE="make_CTA_DST.log"

set cmd2="ls -lh ${MyTmpDir}/"
${cmd2}
set cmd3="df -h ${MyTmpDir}/"
${cmd3}


# Make checks on the generated DST files 
echo ""
echo "> Make checks"
echo ""


# check if we had no triggered events during the dst prod
set val='"DONE"'
set NoTrig=`cat ${CHECKFILE} | grep "no events found" | wc | awk '{print $1}'`
set NoTrig2=`cat ${CHECKFILE} | grep "Failed to find Primary Dataset events" | wc | awk '{print $1}'`
set Error=`cat ${CHECKFILE}  | grep -i "ERROR" | grep -v "BrokenPixel" | wc | awk '{print $1}'`
set Success=`cat ${CHECKFILE} | grep "SUCCESS" | wc | awk '{print $1}'`
if ( $NoTrig != 0 || $NoTrig2 != 0) then
   set val = '"NoTrig"'
else
  if ( ! $Success != 0 ) then
    set val = '"Failed"'
  endif
endif
echo $val
if ( $Error != 0 && $val != '"NoTrig"') then
  set val = '"Failed"'
endif

echo "=============== Checks of the DST prod ======================"

echo "Val $val - NoTrig $NoTrig - NoTrig2 $NoTrig2 - Error $Error"

if ( $val != '"DONE"' ) then

  date
  if ( $val != '"NoTrig"' ) then
    echo ""
    echo " > Big problem during the DST production"
    echo " > FATAL ERROR"
    echo ""
  else
    echo ""
    echo " > No triggered events"
    echo " > Check succeeded"
    echo ""
  endif
  
#  exit  
endif

if ($val == '"DONE"') then
exit 0
else if ($val == '"FAILED"') then
exit 1
else if ($val == '"NoTrig"') then
exit 2
endif 








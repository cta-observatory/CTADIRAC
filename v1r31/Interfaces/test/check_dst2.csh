#! /bin/csh


set CHECKFILE="CheckDST.log"
set MyTmpDir="."
set cmd2="ls -lh ${MyTmpDir}/"


echo "============== Checks of the DST =================="

set Success=`cat ${CHECKFILE}  | grep "Check succeeded" | wc | awk '{print $1}'`
if ( $Success == 0 ) then
  echo "  ### FATAL ERROR: FAILED to make the DST for the given config"
  set val = '"FAILED"'
  ${cmd2}
  echo ""
  echo " > Big problem during the DST production with triggered events"
  echo " > FATAL ERROR"
  echo ""
else
  echo ""
  echo "  > DST done for the given config"
  set val = '"DONE"'
  set NoTrig=`cat ${CHECKFILE}  | grep "no triggered events" | wc | awk '{print $1}'`
  if ( $NoTrig != 0 ) then
    set val = '"NoTrig"'
  endif
    echo ""
    echo " > Check succeeded"
    echo ""
endif


echo "Success $Success - Val $val"

if ($val == '"DONE"') then
exit 0
else if ($val == '"FAILED"') then
exit 1
else if ($val == '"NoTrig"') then
exit 2
endif 


echo "ok1"

date
echo " "

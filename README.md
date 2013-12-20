titan-add-vertex-test
=====================

This project contains a single test.  It's a concurrent stress test.  Beware of false successes.

To run it 250 times in a loop and save the console output from failures (under bash):

rm -f console_*.log; lim=250; for t in `seq 1 $lim` ; do mvn clean test 2>&1 | tee console.log ; grep 'BUILD SUCCESS' console.log >/dev/null ; if [ $? -ne 0 ] ; then cp console.log console_${t}.log ; fi ; done ; echo $t

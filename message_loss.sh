#!/bin/bash -x
OUTFILEPRE=${0%.sh}; OUTFILEPRE=logs/${OUTFILEPRE#./}_${BASHPID}
i=0
while [[ ${i} -lt 100 ]] ; do
 i=$((i+1))
 echo "Starting test ${OUTFILEPRE}_${i}.txt"
 sbt -no-colors 'tests/testOnly akka.kafka.scaladsl.RebalanceSpec -- -z "no message loss during partition revocation and re-subscription"' 2>&1 | tee ${OUTFILEPRE}_${i}.txt
done

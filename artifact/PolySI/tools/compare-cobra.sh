#!/bin/sh
set -x

VARS=10000
OPS=20

for TXNS in 100 200 300 600; do
  cd ../dbcop
  rm -rf clientops/* result_yugabyte/* && cargo run --release generate -d clientops -h 1 -n 15 -t ${TXNS} -e ${OPS} -v ${VARS} && time cargo run --release --example yugabyte 127.0.0.1:5433 127.0.0.1:5434 127.0.0.1:5435 --dir clientops --out result_yugabyte
  cd ../CobraVerifier
  java -jar build/libs/CobraVerifier-0.0.1-SNAPSHOT.jar audit -t dbcop ../dbcop/result_yugabyte/hist-00000/history.bincode
  rm -rf /tmp/cobra/* && java -jar build/libs/CobraVerifier-0.0.1-SNAPSHOT.jar convert -f dbcop -o cobra -t si2ser ~/Source/dbcop/result_yugabyte/hist-00000/history.bincode /tmp/cobra
  java -Djava.library.path=./include:./build/monosat -jar target/CobraVerifier-0.0.1-SNAPSHOT-jar-with-dependencies.jar mono audit cobra.conf.default /tmp/cobra
done

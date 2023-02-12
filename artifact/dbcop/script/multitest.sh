set -e

for i in {1..100}; do
  cargo run --example cockroachdb --release -- -v 7 -t 10 -e 10 172.20.0.2 172.20.0.3 172.20.0.4 1> result/$(date +%s%N).txt 2>&1
done

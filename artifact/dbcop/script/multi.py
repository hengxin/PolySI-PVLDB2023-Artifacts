import argparse
import itertools
import os
import subprocess
import uuid
import time

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--max_var", type=int, default=5)
parser.add_argument("-t", "--max_txn", type=int, default=15)
parser.add_argument("-e", "--max_evt", type=int, default=15)

args = parser.parse_args()

step = 1

tups = itertools.product(range(5, args.max_var + 1, step),
                         range(5, args.max_txn + 1, step),
                         range(5, args.max_evt + 1, step))

tups = sorted(tups, key=lambda x: sum(x))

# index = tups.index((5, 8, 10))
# index = tups.index((5, 8, 15))
index = tups.index((5, 10, 15))
tups = tups[index:]


# 5 var, 3 client, 4 txn, 3 event

for n_var, n_txn, n_evt in tups:
    f_path = "{:03}_{:03}_{:03}_{}".format(
        n_var, n_txn, n_evt, uuid.uuid4().hex[:5])
    os.mkdir(f_path)
    for i in range(250):
        with open(os.path.join(f_path, "{:06}.txt".format(i)), "w") as f:
            # process = subprocess.Popen("../target/release/examples/cockroachdb -v{} -t{} -e{} 172.19.0.2 172.19.0.3 172.19.0.4".format(
            # process = subprocess.Popen("../target/release/examples/galera -v{} -t{} -e{} 172.19.0.4 172.19.0.6 172.19.0.3 172.19.0.7 172.19.0.5".format(
            process = subprocess.Popen("../target/release/examples/antidotedb -v{} -t{} -e{} 172.18.0.2 172.18.0.3 172.18.0.4".format(
                n_var, n_txn, n_evt).split(), stdout=f, stderr=f)
            process.wait()
            # time.sleep(.2)


# for n_var, n_txn, n_evt in itertools.product([5], [4], [3]):
# for n_var, n_txn, n_evt in itertools.product([5], [4], [3]):
#     f_path = "{:03}_{:03}_{:03}_{}".format(
#         n_var, n_txn, n_evt, uuid.uuid4().hex[:5])
#     os.mkdir(f_path)
#     for i in range(1000):
#         with open(os.path.join(f_path, "{:06}.txt".format(i)), "w") as f:
#             # process = subprocess.Popen("../target/release/examples/cockroachdb -v{} -t{} -e{} 172.19.0.7 172.19.0.6 172.19.0.5 172.19.0.4 172.19.0.3".format(
#             # process = subprocess.Popen("../target/release/examples/galera -v{} -t{} -e{} 172.19.0.4 172.19.0.6 172.19.0.3 172.19.0.7 172.19.0.5".format(
#             process = subprocess.Popen("../target/release/examples/antidotedb -v{} -t{} -e{} 172.18.0.2 172.18.0.3 172.18.0.4".format(
#                 n_var, n_txn, n_evt).split(), stdout=f, stderr=f)
#             process.wait()
#             time.sleep(.2)

#!/bin/bash
set -ex -o pipefail

# histories
FIG7_HIST=              # absolute path of PolySIHistories/fig7 (REQUIRED)
FIG8_9_10_HIST=         # absolute path of PolySIHistories/fig8_9_10 (REQUIRED)

# verifiers
COBRA_DIR=              # absolute path of the directory CobraVerifier (optional, will not run Cobra if not set)
DBCOP_DIR=              # absolute path of the directory dbcop (optional, will not run dbcop if not set)
SI_DIR=                 # absolute path of the directory PolySI (REQUIRED)

if [ -z "$FIG7_HIST" ]; then
  echo '$FIG7_HIST' is not set
  exit 1
elif [ -z "$FIG8_9_10_HIST" ]; then
  echo '$FIG8_9_10_HIST' is not set
  exit 1
elif [ -z "$SI_DIR" ]; then
  echo '$SI_DIR' is not set
  exit 1
fi

# outputs
HIST_COBRA_SI_DEST=/tmp/history_cobra_si/
HIST_COBRA_DEST=/tmp/history_cobra/
COBRA_DEST=/tmp/cobra/
COBRA_SI_DEST=/tmp/cobra_si/
COBRA_SI_NOGPU_DEST=/tmp/cobra_si_nogpu/
DBCOP_DEST=/tmp/dbcop/
SI_DEST=/tmp/si/
SI_NO_PRUNING_DEST=/tmp/si-no-pruning/
SI_NO_PRUNING_COALESCING_DEST=/tmp/si-no-pruning-coalescing/
CSV_DEST=/tmp/csv

FIG7A_PARAMS=("5_100_15_10000_0.5_uniform" "10_100_15_10000_0.5_uniform" "15_100_15_10000_0.5_uniform" "20_100_15_10000_0.5_uniform" "25_100_15_10000_0.5_uniform" "30_100_15_10000_0.5_uniform")
FIG7B_PARAMS=("20_10_15_10000_0.5_uniform" "20_20_15_10000_0.5_uniform" "20_30_15_10000_0.5_uniform" "20_40_15_10000_0.5_uniform" "20_50_15_10000_0.5_uniform" "20_100_15_10000_0.5_uniform" "20_150_15_10000_0.5_uniform" "20_200_15_10000_0.5_uniform" "20_250_15_10000_0.5_uniform")
FIG7C_PARAMS=("20_100_5_10000_0.5_uniform" "20_100_10_10000_0.5_uniform" "20_100_15_10000_0.5_uniform" "20_100_20_10000_0.5_uniform" "20_100_25_10000_0.5_uniform" "20_100_30_10000_0.5_uniform")
FIG7D_PARAMS=("20_100_15_10000_0_uniform" "20_100_15_10000_0.25_uniform" "20_100_15_10000_0.5_uniform" "20_100_15_10000_0.75_uniform" "20_100_15_10000_1_uniform")
FIG7E_PARAMS=("20_100_15_1000_0.5_uniform" "20_100_15_2000_0.5_uniform" "20_100_15_3000_0.5_uniform" "20_100_15_4000_0.5_uniform" "20_100_15_6000_0.5_uniform" "20_100_15_8000_0.5_uniform" "20_100_15_10000_0.5_uniform")
FIG7F_PARAMS=("20_100_15_10000_0.5_uniform" "20_100_15_10000_0.5_zipf" "20_100_15_10000_0.5_hotspot")

rm -rf $HIST_COBRA_SI_DEST $HIST_COBRA_DEST $COBRA_DEST $COBRA_SI_DEST $COBRA_SI_NOGPU_DEST $DBCOP_DEST $SI_DEST $SI_NO_PRUNING_DEST $SI_NO_PRUNING_COALESCING_DEST $CSV_DEST

cat > /tmp/cobra.conf.nogpu <<EOF
HEAVY_VALIDATION_CODE_ON=false
MULTI_THREADING_OPT=true
TIME_ORDER_ON=false
INFER_RELATION_ON=true
PCSG_ON=true
WRITE_SPACE_ON=true
MERGE_CONSTRAINT_ON=false
LOGGER_ON_SCREEN=true
LOGGER_PATH=/tmp/cobra/logger.log
LOGGER_LEVEL=INFO
LOG_FD_LOG=/tmp/cobra/log/
FETCHING_DURATION_BASE=500
FETCHING_DURATION_RAND=500
NUM_BATCH_FETCH_TRACE=1000
ONLINE_DB_TYPE=2
BENCH_TYPE=2
DUMP_POLYG=false
MAX_INFER_ROUNDS=1
BUNDLE_CONSTRAINTS=true
WW_CONSTRAINTS=true
BATCH_TX_VERI_SIZE=100
GPU_MATRIX=false
TOTAL_CLIENTS=24
DB_HOST=ye-cheng.duckdns.org
MIN_PROCESSING_NEW_TXN=5000
GC_EPOCH_THRESHOLD=100
TIME_DRIFT_THRESHOLD=100
EOF

avg_time() {
  local dest="$1"
  local param="$2"
  local part="$3"

  case "$dest" in
    "$COBRA_DEST" | "$COBRA_SI_DEST" | "$COBRA_SI_NOGPU_DEST")
      local pattern='s/^\[INFO \] >>> Overall runtime = ([[:digit:]]+)ms$/\1/p'
      ;;
    "$SI_DEST" | "$SI_NO_PRUNING_DEST" | "$SI_NO_PRUNING_COALESCING_DEST")
      case "$part" in
        "")
          local pattern='s/^ENTIRE_EXPERIMENT: ([[:digit:]]+)ms$/\1/p'
          ;;
        pruning)
          local pattern='s/^SI_PRUNE: ([[:digit:]]+)ms$/\1/p'
          ;;
        constructing)
          local pattern='s/^ONESHOT_CONS: ([[:digit:]]+)ms$/\1/p'
          ;;
        solving)
          local pattern='s/^ONESHOT_SOLVE: ([[:digit:]]+)ms$/\1/p'
          ;;
        *)
          exit 1
          ;;
      esac
      ;;
    "$DBCOP_DEST")
      local pattern='s/.+"duration":([[:digit:]]+\.[[:digit:]]+).+/\1/p'
      ;;
    *)
      exit 1
      ;;
  esac

  local time=()
  for hist in $(find "$dest/$param" -name "hist-*"); do
    n="$(cat $hist | sed -nE "$pattern")"
    if [ -n "$n" ]; then
      time+=("$(bc <<<"scale=4; $n / 1000")")
    else
      time+=(180)
    fi
  done

  echo "$(IFS='+'; echo "scale=4; (${time[*]}) / ${#time[@]}" | bc)"
}

run() {
  local prog="$1"
  local hist="$2"
  local out="$3"

  mkdir -p "$(dirname "$out")"

  case "$prog" in
    si)
      if [ -e "$hist/history.bincode" ]; then
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" audit -t dbcop "$hist/history.bincode" &> "$out" || true
      else
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" audit "$hist" &> "$out" || true
      fi
      ;;
    si-no-pruning)
      if [ -e "$hist/history.bincode" ]; then
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" audit -t dbcop --no-pruning "$hist/history.bincode" &> "$out" || true
      else
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" audit --no-pruning "$hist" &> "$out" || true
      fi
      ;;
    si-no-pruning-coalescing)
      if [ -e "$hist/history.bincode" ]; then
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" audit -t dbcop --no-pruning --no-coalescing "$hist/history.bincode" &> "$out" || true
      else
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" audit --no-pruning --no-coalescing "$hist" &> "$out" || true
      fi
      ;;
    cobra)
      timeout 180 java "-Djava.library.path=$COBRA_DIR/include/:$COBRA_DIR/build/monosat" -jar "$COBRA_DIR/target/CobraVerifier-0.0.1-SNAPSHOT-jar-with-dependencies.jar" mono audit "$COBRA_DIR/cobra.conf.default" "$hist" &> "$out" || true
      ;;
    cobra-nogpu)
      timeout 180 java "-Djava.library.path=$COBRA_DIR/include/:$COBRA_DIR/build/monosat" -jar "$COBRA_DIR/target/CobraVerifier-0.0.1-SNAPSHOT-jar-with-dependencies.jar" mono audit "/tmp/cobra.conf.nogpu" "$hist" &> "$out" || true
      ;;
    dbcop)
      local base="$(dirname "$out")"
      timeout 180 $DBCOP_DIR/target/release/dbcop verify --cons si --ver_dir "$hist" --out_dir "$base" >/dev/null || true
      mv "$base/result_log.json" "$out"
      ;;
    *)
  esac
}

split_fig7() {
  local out="$1"
  shift
  head -n1 "$CSV_DEST/fig7.csv" >> "$out"
  for p in "$@"; do
    sed -n "/$p/p" "$CSV_DEST/fig7.csv" >> "$out"
  done
}

reproduce_fig7() {
  local params=$(ls "$FIG7_HIST")
  local -A avg_time_si=()
  local -A avg_time_dbcop=()
  local -A avg_time_cobra_si=()
  local -A avg_time_cobra_si_nogpu=()

  for p in ${params[@]}; do
    for hist in $(find "$FIG7_HIST/$p" -name "hist-*"); do
      local si2ser_hist="${hist/$FIG7_HIST/$HIST_COBRA_SI_DEST}"
      mkdir -p "$si2ser_hist"
      java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" convert -f dbcop -o cobra -t si2ser "$hist/history.bincode" "$si2ser_hist"

      run si "$hist" "${hist/$FIG7_HIST/$SI_DEST}"
      if [ -n "$COBRA_DIR" ]; then
        run cobra "$si2ser_hist" "${hist/$FIG7_HIST/$COBRA_SI_DEST}"
        run cobra-nogpu "$si2ser_hist" "${hist/$FIG7_HIST/$COBRA_SI_NOGPU_DEST}"
      fi
      if [ -n "$DBCOP_DIR" ]; then
        run dbcop "$hist" "${hist/$FIG7_HIST/$DBCOP_DEST}"
      fi
    done

    avg_time_si[$p]="$(avg_time "$SI_DEST" $p)"
    if [ -n "$COBRA_DIR" ]; then
      avg_time_cobra_si[$p]="$(avg_time "$COBRA_SI_DEST" $p)"
      avg_time_cobra_si_nogpu[$p]="$(avg_time "$COBRA_SI_NOGPU_DEST" $p)"
    fi
    if [ -n "$DBCOP_DIR" ]; then
      avg_time_dbcop[$p]="$(avg_time "$DBCOP_DEST" $p)"
    fi
  done

  mkdir -p $CSV_DEST
  echo "param,cobra(si),cobra(si-nogpu),si,oopsla" > "$CSV_DEST/fig7.csv"
  for p in ${params[@]}; do
    echo "$p,${avg_time_cobra_si[$p]},${avg_time_cobra_si_nogpu[$p]},${avg_time_si[$p]},${avg_time_dbcop[$p]}" >> "$CSV_DEST/fig7.csv"
  done

  split_fig7 "$CSV_DEST/fig7a.csv" ${FIG7A_PARAMS[@]}
  split_fig7 "$CSV_DEST/fig7b.csv" ${FIG7B_PARAMS[@]}
  split_fig7 "$CSV_DEST/fig7c.csv" ${FIG7C_PARAMS[@]}
  split_fig7 "$CSV_DEST/fig7d.csv" ${FIG7D_PARAMS[@]}
  split_fig7 "$CSV_DEST/fig7e.csv" ${FIG7E_PARAMS[@]}
  split_fig7 "$CSV_DEST/fig7f.csv" ${FIG7F_PARAMS[@]}
}

reproduce_fig8_9_10() {
  local params=$(ls "$FIG8_9_10_HIST")
  local -A avg_time_si=()
  local -A avg_time_si_constructing=()
  local -A avg_time_si_pruning=()
  local -A avg_time_si_solving=()
  local -A avg_time_si_no_pruning=()
  local -A avg_time_si_no_pruning_coalescing=()
  local -A avg_time_cobra=()

  for p in ${params[@]}; do
    for hist in $(find "$FIG8_9_10_HIST/$p" -name "hist-*"); do
      if [ -e "$hist/history.bincode" ]; then
        local cobra_hist="${hist/$FIG8_9_10_HIST/$HIST_COBRA_DEST}"
        mkdir -p "$cobra_hist"
        java -jar "$SI_DIR/build/libs/PolySI-1.0.0-SNAPSHOT.jar" convert -f dbcop -o cobra -t identity "$hist/history.bincode" "$cobra_hist"
      else
        local cobra_hist="$hist"
      fi

      run si "$hist" "${hist/$FIG8_9_10_HIST/$SI_DEST}"
      run si-no-pruning "$hist" "${hist/$FIG8_9_10_HIST/$SI_NO_PRUNING_DEST}"
      run si-no-pruning-coalescing "$hist" "${hist/$FIG8_9_10_HIST/$SI_NO_PRUNING_COALESCING_DEST}"

      if [ -n "$COBRA_DIR" ]; then
        run cobra "$cobra_hist" "${hist/$FIG8_9_10_HIST/$COBRA_DEST}"
      fi
    done

    avg_time_si[$p]="$(avg_time "$SI_DEST" $p)"
    avg_time_si_constructing[$p]="$(avg_time "$SI_DEST" $p constructing)"
    avg_time_si_pruning[$p]="$(avg_time "$SI_DEST" $p pruning)"
    avg_time_si_solving[$p]="$(avg_time "$SI_DEST" $p solving)"
    avg_time_si_no_pruning[$p]="$(avg_time "$SI_NO_PRUNING_DEST" $p)"
    avg_time_si_no_pruning_coalescing[$p]="$(avg_time "$SI_NO_PRUNING_COALESCING_DEST" $p)"
    if [ -n "$COBRA_DIR" ]; then
      avg_time_cobra[$p]="$(avg_time "$COBRA_DEST" $p)"
    fi
  done

  mkdir -p $CSV_DEST
  echo "param,si,si(no-pruning),si(no-pruning-coalescing),cobra" > "$CSV_DEST/fig8_10.csv"
  for p in ${params[@]}; do
    echo "$p,${avg_time_si[$p]},${avg_time_si_no_pruning[$p]},${avg_time_si_no_pruning_coalescing[$p]},${avg_time_cobra[$p]}" >> "$CSV_DEST/fig8_10.csv"
  done

  mkdir -p $CSV_DEST
  echo "param,constructing,pruning,solving" > "$CSV_DEST/fig9.csv"
  for p in ${params[@]}; do
    echo "$p,${avg_time_si_constructing[$p]},${avg_time_si_pruning[$p]},${avg_time_si_solving[$p]}" >> "$CSV_DEST/fig9.csv"
  done
}

reproduce_fig7
reproduce_fig8_9_10

# PolySI

This repo contains the software artifact of PolySI, including:

1. The source code of PolySI, which is our implementation of the algorithms
   described in the paper.
2. The source code of [Cobra](https://github.com/DBCobra/CobraVerifier).
3. The source code of [dbcop](https://zenodo.org/record/3370437#.YhjFofXMLlx).
4. The collected histories used to reproduce the results in the paper.

## Requirements

The artifact was tested on Ubuntu 22.04. The steps to build and run each of
these verifiers are shown in their own READMEs.

## Reproducing results

1. Build PolySI, Cobra and dbcop

   Please follow their instructions in respective READMEs.

   Cobra and dbcop are optional. If you don't build one of them, then the
   results will not be included in the output.

2. Modify the paths in `repro/reproduce.sh` to point to the directories of
   histories and verifiers

   The variables that needs to be modified are: `FIG7_HIST`, `FIG8_9_10_HIST`,
   `COBRA_DIR`, `DBCOP_DIR`, `SI_DIR`. They are shown in the first few lines of
   the script.

   `COBRA_DIR` and `DBCOP_DIR` should be left empty if the corresponding program
   is not built.

3. Run `repro/reproduce.sh`

   The results are stored in `/tmp/csv` in csv format. For figure 7, the first
   column is the parameters used to generate the history, and is formatted as
   `${#sessions}_${#txns/session}_${#ops/txn}_${#keys}_${read_probability}_${key_distribution}`.

   Running the entire experiment takes a few hours.

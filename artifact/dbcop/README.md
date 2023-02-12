We use the release build when running benchmarks.

To build:

```
sudo apt install cargo
cargo b --release
```

# DBCop

## Usage

1.  Clone it.
```
    git clone git@gitlab.math.univ-paris-diderot.fr:ranadeep/dbcop.git
```

2.  Compile and install using `cargo` and run.
    Make sure `~/.cargo/bin` is in your system path.
```
    cd dbcop
    dbcop install --path .
    dbcop --help
```
---

There are a few `docker-compose` files in `docker` directory to create docker cluster.

The workflow goes like this,

1. Generate a bunch of histories to execute on a database.
2. Execute those histories on a database using provided `traits`. (see in `examples`).
3. Verify the executed histories for `--cc`(causal consistency), `--si`(snapshot isolation), `--ser`(serialization).  

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import history.Event;
import history.Event.EventType;
import history.History;
import history.HistoryLoader;
import history.HistoryParser;
import history.HistoryTransformer;
import history.Transaction;
import history.loaders.CobraHistoryLoader;
import history.loaders.DBCopHistoryLoader;
import history.loaders.ElleHistoryLoader;
import history.loaders.TextHistoryLoader;
import history.transformers.Identity;
import history.transformers.SnapshotIsolationToSerializable;
import lombok.SneakyThrows;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import util.Profiler;
import util.UnimplementedError;
import verifier.Pruning;
import verifier.SIVerifier;

@Command(name = "verifier", mixinStandardHelpOptions = true, version = "verifier 0.0.1", subcommands = { Audit.class,
        Convert.class, Stat.class, Dump.class })
public class Main implements Callable<Integer> {
    @SneakyThrows
    public static void main(String[] args) {
        var cmd = new CommandLine(new Main());
        cmd.setCaseInsensitiveEnumValuesAllowed(true);
        System.exit(cmd.execute(args));
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.err);
        return -1;
    }
}

@Command(name = "audit", mixinStandardHelpOptions = true, description = "Verify a history")
class Audit implements Callable<Integer> {
    @Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Option(names = { "--no-pruning" }, description = "disable pruning")
    private final Boolean noPruning = false;

    @Option(names = { "--no-coalescing" }, description = "disable coalescing")
    private final Boolean noCoalescing = false;

    @Option(names = { "--dot-output" }, description = "print conflicts in dot format")
    private final Boolean dotOutput = false;

    @Parameters(description = "history path")
    private Path path;

    private final Profiler profiler = Profiler.getInstance();

    @Override
    public Integer call() {
        var loader = Utils.getLoader(type, path);

        Pruning.setEnablePruning(!noPruning);
        SIVerifier.setCoalesceConstraints(!noCoalescing);
        SIVerifier.setDotOutput(dotOutput);

        profiler.startTick("ENTIRE_EXPERIMENT");
        var pass = true;
        var verifier = new SIVerifier<>(loader);
        pass = verifier.audit();
        profiler.endTick("ENTIRE_EXPERIMENT");

        for (var p : profiler.getDurations()) {
            System.err.printf("%s: %dms\n", p.getKey(), p.getValue());
        }
        System.err.printf("Max memory: %s\n", Utils.formatMemory(profiler.getMaxMemory()));

        if (pass) {
            System.err.println("[[[[ ACCEPT ]]]]");
            return 0;
        } else {
            System.err.println("[[[[ REJECT ]]]]");
            return -1;
        }
    }
}

@Command(name = "convert", mixinStandardHelpOptions = true, description = "Convert a history between different formats")
class Convert implements Callable<Integer> {
    @Option(names = { "-f", "--from" }, description = "input history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType inType = HistoryType.COBRA;

    @Option(names = { "-o", "--output" }, description = "input history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType outType = HistoryType.DBCOP;

    @Option(names = { "-t", "--transform" }, description = "history transformation: ${COMPLETION-CANDIDATES}")
    private final HistoryTransformation transformation = HistoryTransformation.IDENTITY;

    @Parameters(description = "input history path", index = "0")
    private Path inPath;

    @Parameters(description = "output history path", index = "1")
    private Path outPath;

    @Override
    public Integer call() {
        var in = Utils.getLoader(inType, inPath);
        var out = Utils.getLoader(outType, outPath);
        var transformer = Utils.getTransformer(transformation);

        var history = in.loadHistory();
        history = transformer.transformHistory(history);

        if (!(out instanceof HistoryParser)) {
            throw new RuntimeException(String.format("Conversion not supported for %s", out.getClass().getName()));
        }
        convertAndDump((HistoryParser<?, ?>) out, history);

        return 0;
    }

    private <T, U> void convertAndDump(HistoryParser<T, U> parser, History<?, ?> history) {
        parser.dumpHistory(parser.convertFrom(history));
    }
}

@Command(name = "stat", mixinStandardHelpOptions = true, description = "Print some statistics of a history")
class Stat implements Callable<Integer> {
    @Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Parameters(description = "history path")
    private Path path;

    @Override
    public Integer call() {
        var loader = Utils.getLoader(type, path);
        var history = loader.loadHistory();

        var txns = history.getTransactions();
        var events = history.getEvents();
        var writeFreq = events.stream()
                .collect(Collectors.toMap(ev -> ev.getKey(), ev -> ev.getType().equals(EventType.WRITE) ? 1 : 0,
                        Integer::sum))
                .entrySet().stream().collect(Collectors.toMap(w -> w.getValue(), w -> 1, Integer::sum)).entrySet()
                .stream().sorted((p, q) -> Integer.compare(p.getKey(), q.getKey()))
                .collect(Collectors.toCollection(ArrayList::new));

        System.out.printf(
                "Sessions: %d\n" + "Transactions: %d, read-only: %d, write-only: %d, read-modify-write: %d\n"
                        + "Events: total %d, read %d, write %d\n" + "Variables: %d\n",
                history.getSessions().size(), history.getTransactions().size(),
                txns.stream().filter(txn -> txn.getEvents().stream().allMatch(ev -> ev.getType() == EventType.READ))
                        .count(),
                txns.stream().filter(txn -> txn.getEvents().stream().allMatch(ev -> ev.getType() == EventType.WRITE))
                        .count(),
                txns.stream().filter(Stat::isReadModifyWriteTxn).count(), events.size(),
                events.stream().filter(e -> e.getType() == Event.EventType.READ).count(),
                events.stream().filter(e -> e.getType() == Event.EventType.WRITE).count(),
                events.stream().map(e -> e.getKey()).distinct().count());

        System.out.println("(writes, #keys):");
        int min = writeFreq.get(0).getKey(), max = writeFreq.get(writeFreq.size() - 1).getKey();
        int step = Math.max((max - min) / 8, 1), lowerBound;

        if (writeFreq.get(0).getKey() == 1) {
            System.out.printf("1: %d\n", writeFreq.get(0).getValue());
            lowerBound = 2;
        } else {
            lowerBound = 1;
        }
        for (; lowerBound <= max; lowerBound += step) {
            int x = lowerBound;
            int count = writeFreq.stream().filter(w -> x <= w.getKey() && w.getKey() < x + step)
                    .mapToInt(w -> w.getValue()).sum();
            System.out.printf("%d...%d: %d\n", lowerBound, lowerBound + step - 1, count);
        }

        return 0;
    }

    private static boolean isReadModifyWriteTxn(Transaction<?, ?> txn) {
        var readKeys = new HashSet<Object>();
        for (var ev : txn.getEvents()) {
            if (ev.getType().equals(EventType.READ)) {
                readKeys.add(ev.getKey());
            } else if (readKeys.contains(ev.getKey())) {
                return true;
            }
        }

        return false;
    }
}

@Command(name = "dump", mixinStandardHelpOptions = true, description = "Print a history to stdout")
class Dump implements Callable<Integer> {
    @Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Parameters(description = "history path")
    private Path path;

    @Override
    public Integer call() {
        var loader = Utils.getLoader(type, path);
        var history = loader.loadHistory();

        for (var session : history.getSessions()) {
            for (var txn : session.getTransactions()) {
                var events = txn.getEvents();
                System.out.printf("Transaction %s\n", txn);
                for (var j = 0; j < events.size(); j++) {
                    var ev = events.get(j);
                    System.out.printf("%s\n", ev);
                }
                System.out.println();
            }
        }

        return 0;
    }

}

class Utils {
    static HistoryLoader<?, ?> getLoader(HistoryType type, Path path) {
        switch (type) {
        case COBRA:
            return new CobraHistoryLoader(path);
        case DBCOP:
            return new DBCopHistoryLoader(path);
        case TEXT:
            return new TextHistoryLoader(path);
        case ELLE:
            return new ElleHistoryLoader(path);
        default:
            throw new UnimplementedError();
        }

    }

    static HistoryTransformer getTransformer(HistoryTransformation transform) {
        switch (transform) {
        case IDENTITY:
            return new Identity();
        case SI2SER:
            return new SnapshotIsolationToSerializable();
        default:
            throw new UnimplementedError();
        }
    }

    static String formatMemory(Long memoryBytes) {
        double[] scale = { 1, 1024, 1024 * 1024, 1024 * 1024 * 1024 };
        String[] unit = { "B", "KB", "MB", "GB" };

        for (int i = scale.length - 1; i >= 0; i--) {
            if (i == 0 || memoryBytes >= scale[i]) {
                return String.format("%.1f%s", memoryBytes / scale[i], unit[i]);
            }
        }
        throw new Error("should not be here");
    }
}

enum HistoryType {
    COBRA, DBCOP, TEXT, ELLE
}

enum HistoryTransformation {
    IDENTITY, SI2SER
}

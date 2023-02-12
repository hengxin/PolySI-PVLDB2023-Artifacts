package verifier;

import graph.KnownGraph;
import history.Event;
import history.History;
import history.Transaction;
import history.Event.EventType;
import history.loaders.ElleHistoryLoader.ElleValue;
import util.Profiler;
import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import lombok.Setter;

public class Pruning {
    @Getter
    @Setter
    private static boolean enablePruning = true;

    @Getter
    @Setter
    private static double stopThreshold = 0.01;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <KeyType, ValueType> boolean pruneConstraints(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints, History<KeyType, ValueType> history) {
        if (!enablePruning) {
            return false;
        }

        var profiler = Profiler.getInstance();
        profiler.startTick("SI_PRUNE");

        boolean result;
        if (history.getEvents().iterator().next().getValue() instanceof ElleValue) {
            result = pruneListHistory((KnownGraph) knownGraph, (Collection) constraints, (History) history);
        } else {
            result = pruneKeyValueHistory(knownGraph, constraints, history);
        }

        profiler.endTick("SI_PRUNE");
        return result;
    }

    private static <KeyType, ValueType> boolean pruneKeyValueHistory(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints, History<KeyType, ValueType> history) {
        int rounds = 1, solvedConstraints = 0, totalConstraints = constraints.size();
        boolean hasCycle = false;
        while (!hasCycle) {
            System.err.printf("Pruning round %d\n", rounds);
            var result = pruneConstraintsWithPostChecking(knownGraph, constraints, history);

            hasCycle = result.getRight();
            solvedConstraints += result.getLeft();

            if (result.getLeft() <= stopThreshold * totalConstraints
                    || totalConstraints - solvedConstraints <= stopThreshold * totalConstraints) {
                break;
            }
            rounds++;
        }

        System.err.printf("Pruned %d rounds, solved %d constraints\n" + "After prune: graphA: %d, graphB: %d\n", rounds,
                solvedConstraints, knownGraph.getKnownGraphA().edges().size(),
                knownGraph.getKnownGraphB().edges().size());
        return hasCycle;
    }

    private static <KeyType> boolean pruneListHistory(KnownGraph<KeyType, ElleValue> knownGraph,
            Collection<SIConstraint<KeyType, ElleValue>> constraints, History<KeyType, ElleValue> history) {
        var readResults = new HashMap<KeyType, List<List<Integer>>>();
        history.getEvents().stream().filter(e -> e.getType() == EventType.READ).forEach(
                e -> readResults.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(e.getValue().getList()));

        for (var e : readResults.entrySet()) {
            var key = e.getKey();
            var values = e.getValue();

            values.sort((a, b) -> Integer.compare(a.size(), b.size()));
            for (int i = 0; i < values.size() - 1; i++) {
                var prev = values.get(i);
                var next = values.get(i + 1);

                for (int j = 0; j < prev.size(); j++) {
                    if (!prev.get(j).equals(next.get(j))) {
                        System.err.printf("Inconsistent list between [:r %d %s] and [:r %d %s]\n", key, prev, key,
                                next);
                        return true;
                    }
                }
            }
        }

        var finalLists = readResults.entrySet().stream().collect(Collectors.toMap(Entry::getKey, p -> {
            var l = p.getValue();
            return l.get(l.size() - 1);
        }));
        var txnWrites = history.getTransactions().stream()
                .flatMap(
                        t -> t.getEvents().stream().filter(e -> e.getType() == EventType.WRITE).map(e -> Pair.of(t, e)))
                .collect(Collectors.toMap(p -> Pair.of(p.getLeft(), p.getRight().getKey()), p -> {
                    var value = p.getRight().getValue().getLastElement();
                    return value == null ? Optional.empty() : Optional.of(value);
                }, (a, b) -> a));

        var solvedConstraints = new ArrayList<Pair<SIConstraint<KeyType, ElleValue>, Transaction<KeyType, ElleValue>>>();
        for (var c : constraints) {
            var t1 = c.getWriteTransaction1();
            var t2 = c.getWriteTransaction2();

            var keySet = ((Function<Transaction<KeyType, ElleValue>, Set<KeyType>>) t -> t.getEvents().stream()
                    .filter(e -> e.getType() == EventType.WRITE).map(Event::getKey).collect(Collectors.toSet()));
            var bothWrittenKeys = Sets.intersection(keySet.apply(c.getWriteTransaction1()),
                    keySet.apply(c.getWriteTransaction2()));

            for (var k : bothWrittenKeys) {
                var value1 = txnWrites.get(Pair.of(t1, k));
                var value2 = txnWrites.get(Pair.of(t2, k));

                if (value1.isEmpty() || value2.isEmpty()) {
                    assert value1.isPresent() || value2.isPresent();

                    if (value1.isEmpty()) {
                        solvedConstraints.add(Pair.of(c, t1));
                    } else {
                        solvedConstraints.add(Pair.of(c, t2));
                    }
                    break;
                }

                var finalList = finalLists.get(k);
                if (finalList == null) {
                    continue;
                }

                var index1 = finalList.indexOf(value1.get());
                var index2 = finalList.indexOf(value2.get());
                if (index1 == -1 && index2 == -1) {
                    continue;
                }

                if (index1 == -1 || index2 == -1) {
                    if (index1 == -1) {
                        solvedConstraints.add(Pair.of(c, t2));
                    } else {
                        solvedConstraints.add(Pair.of(c, t1));
                    }
                    break;
                }

                if (index1 < index2) {
                    solvedConstraints.add(Pair.of(c, t1));
                } else {
                    solvedConstraints.add(Pair.of(c, t2));
                }
                break;
            }
        }

        System.err.printf("Solved %d of %d constraints\n", solvedConstraints.size(), constraints.size());

        for (var p : solvedConstraints) {
            var c = p.getLeft();
            constraints.remove(c);

            if (p.getRight() == c.getWriteTransaction1()) {
                addToKnownGraph(knownGraph, c.getEdges1());
            } else {
                addToKnownGraph(knownGraph, c.getEdges2());
            }
        }

        return false;
    }

    private static <KeyType, ValueType> Pair<Integer, Boolean> pruneConstraintsWithPostChecking(
            KnownGraph<KeyType, ValueType> knownGraph, Collection<SIConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA().asGraph());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB().asGraph(), graphA.getNodeMap());
        var orderInSession = Utils.getOrderInSession(history);
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        var graphC = graphA.composition(graphB);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        if (graphC.hasLoops()) {
            return Pair.of(0, true);
        }

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        var reachability = Utils.reduceEdges(graphA.union(graphC), orderInSession).reachability();
        System.err.printf("reachability matrix sparsity: %.2f\n",
                1 - reachability.nonZeroElements() / Math.pow(reachability.nodes().size(), 2));
        profiler.endTick("SI_PRUNE_POST_REACHABILITY");

        var solvedConstraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        profiler.startTick("SI_PRUNE_POST_CHECK");
        for (var c : constraints) {
            var conflict = checkConflict(c.getEdges1(), reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.getEdges2());
                solvedConstraints.add(c);
                // System.err.printf("%s -> %s because of conflict in %s\n",
                // c.writeTransaction2, c.writeTransaction1,
                // conflict.get());
                continue;
            }

            conflict = checkConflict(c.getEdges2(), reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.getEdges1());
                // System.err.printf("%s -> %s because of conflict in %s\n",
                // c.writeTransaction1, c.writeTransaction2,
                // conflict.get());
                solvedConstraints.add(c);
            }
        }
        profiler.endTick("SI_PRUNE_POST_CHECK");

        System.err.printf("solved %d constraints\n", solvedConstraints.size());
        // constraints.removeAll(solvedConstraints);
        // java removeAll has performance bugs; do it manually
        solvedConstraints.forEach(constraints::remove);
        return Pair.of(solvedConstraints.size(), false);
    }

    private static <KeyType, ValueType> void addToKnownGraph(KnownGraph<KeyType, ValueType> knownGraph,
            Collection<SIEdge<KeyType, ValueType>> edges) {
        for (var e : edges) {
            switch (e.getType()) {
            case WW:
                knownGraph.putEdge(e.getFrom(), e.getTo(), new Edge<KeyType>(EdgeType.WW, e.getKey()));
                break;
            case RW:
                knownGraph.putEdge(e.getFrom(), e.getTo(), new Edge<KeyType>(EdgeType.RW, e.getKey()));
                break;
            default:
                throw new Error("only WW and RW edges should appear in constraints");
            }
        }
    }

    private static <KeyType, ValueType> Optional<SIEdge<KeyType, ValueType>> checkConflict(
            Collection<SIEdge<KeyType, ValueType>> edges, MatrixGraph<Transaction<KeyType, ValueType>> reachability,
            KnownGraph<KeyType, ValueType> knownGraph) {
        for (var e : edges) {
            switch (e.getType()) {
            case WW:
                if (reachability.hasEdgeConnecting(e.getTo(), e.getFrom())) {
                    return Optional.of(e);
                    // System.err.printf("conflict edge: %s\n", e);
                }
                break;
            case RW:
                for (var n : knownGraph.getKnownGraphA().predecessors(e.getFrom())) {
                    if (reachability.hasEdgeConnecting(e.getTo(), n)) {
                        return Optional.of(e);
                        // System.err.printf("conflict edge: %s\n", e);
                    }
                }
                break;
            default:
                throw new Error("only WW and RW edges should appear in constraints");
            }
        }

        return Optional.empty();
    }
}

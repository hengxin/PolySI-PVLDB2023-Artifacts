package history.transformers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import history.History;
import history.HistoryTransformer;
import history.Transaction;
import history.Event.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class SnapshotIsolationToSerializable implements HistoryTransformer {
    @Override
    public <T, U> History<Object, Object> transformHistory(
            History<T, U> history) {
        var writes = new HashMap<T, Set<Transaction<T, U>>>();
        var writePosition = new HashMap<Pair<T, U>, Pair<Transaction<T, U>, Integer>>();
        history.getTransactions().forEach(txn -> {
            for (int i = 0; i < txn.getEvents().size(); i++) {
                var ev = txn.getEvents().get(i);
                if (ev.getType() != EventType.WRITE) {
                    continue;
                }

                writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>())
                        .add(ev.getTransaction());
                writePosition.put(Pair.of(ev.getKey(), ev.getValue()),
                        Pair.of(txn, i));
            }
        });

        var generator = new KVGenerator();
        var conflictKeys = new HashMap<Pair<Transaction<T, U>, Transaction<T, U>>, Triple<GeneratedKey, GeneratedValue, GeneratedValue>>();
        writes.values().stream().map(ArrayList::new).forEach(list -> {
            for (int i = 0; i < list.size(); i++) {
                for (int j = 0; j < list.size(); j++) {
                    if (i == j) {
                        continue;
                    }

                    conflictKeys.put(Pair.of(list.get(i), list.get(j)),
                            Triple.of(generator.key(), generator.value(),
                                    generator.value()));
                }
            }
        });

        var newHistory = new History<>();
        for (var session : history.getSessions()) {
            var newSession = newHistory.addSession(session.getId());
            for (var txn : session.getTransactions()) {
                var readTxn = newHistory.addTransaction(newSession,
                        txn.getId() * 2);
                var writeTxn = newHistory.addTransaction(newSession,
                        txn.getId() * 2 + 1);
                var conflictTxns = new HashSet<Transaction<T, U>>();

                readTxn.setStatus(txn.getStatus());
                writeTxn.setStatus(txn.getStatus());

                for (var i = 0; i < txn.getEvents().size(); i++) {
                    var op = txn.getEvents().get(i);

                    if (op.getType() == EventType.READ) {
                        var writePos = writePosition
                                .get(Pair.of(op.getKey(), op.getValue()));
                        if (writePos != null && writePos.getLeft() == txn
                                && writePos.getRight() < i) {
                            continue;
                        }

                        newHistory.addEvent(readTxn, EventType.READ,
                                op.getKey(), op.getValue());
                    } else {
                        newHistory.addEvent(writeTxn, EventType.WRITE,
                                op.getKey(), op.getValue());
                        conflictTxns.addAll(writes.get(op.getKey()));
                    }
                }

                conflictTxns.forEach(txn2 -> {
                    if (txn2 == txn) {
                        return;
                    }

                    var op1 = conflictKeys.get(Pair.of(txn, txn2));
                    var op2 = conflictKeys.get(Pair.of(txn2, txn));
                    newHistory.addEvent(readTxn, EventType.WRITE, op1.getLeft(),
                            op1.getMiddle());
                    newHistory.addEvent(writeTxn, EventType.READ, op1.getLeft(),
                            op1.getMiddle());
                    newHistory.addEvent(writeTxn, EventType.WRITE,
                            op2.getLeft(), op2.getRight());
                });
            }
        }

        return newHistory;
    }

    private static class KVGenerator {
        private GeneratedKey key = new GeneratedKey(0);
        private GeneratedValue value = new GeneratedValue(0);

        GeneratedKey key() {
            return this.key = new GeneratedKey(this.key.key - 1);
        }

        GeneratedValue value() {
            return this.value = new GeneratedValue(this.value.value - 1);
        }
    }

    @Data
    @EqualsAndHashCode
    @AllArgsConstructor
    private static class GeneratedKey {
        private final long key;
    }

    @Data
    @EqualsAndHashCode
    @AllArgsConstructor
    private static class GeneratedValue {
        private final long value;
    }
}

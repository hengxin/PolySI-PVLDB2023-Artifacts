package history;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class History<KeyType, ValueType> {
	private final Map<Long, Session<KeyType, ValueType>> sessions = new HashMap<>();
	private final Map<Long, Transaction<KeyType, ValueType>> transactions = new HashMap<>();
	private final Set<Pair<KeyType, ValueType>> writes = new HashSet<>();

	public History(Set<Long> sessions,
			Map<Long, List<Long>> transactions,
			Map<Long, List<Triple<Event.EventType, KeyType, ValueType>>> events) {
		var sessionMap = sessions.stream()
			.map(id -> Pair.of(id, addSession(id)))
			.collect(Collectors.toMap(Pair::getKey, Pair::getValue));

		var txnMap = transactions.entrySet().stream()
			.flatMap(e -> e.getValue().stream().map(id -> {
				var s = sessionMap.get(e.getKey());
				return Pair.of(id, addTransaction(s, id));
			})).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

		events.forEach((id, list) -> list.forEach(e -> addEvent(
			txnMap.get(id), e.getLeft(), e.getMiddle(), e.getRight()
		)));
	}

	public Collection<Session<KeyType, ValueType>> getSessions() {
		return sessions.values();
	}

	public Collection<Transaction<KeyType, ValueType>> getTransactions() {
		return transactions.values();
	}

	public Collection<Event<KeyType, ValueType>> getEvents() {
		return transactions.values().stream().flatMap(txn -> txn.events.stream()).collect(Collectors.toList());
	}

	public Session<KeyType, ValueType> getSession(long id) {
		return sessions.get(id);
	}

	public Transaction<KeyType, ValueType> getTransaction(long id) {
		return transactions.get(id);
	}

	public Session<KeyType, ValueType> addSession(long id) {
		if (sessions.containsKey(id)) {
			throw new InvalidHistoryError();
		}

		var session = new Session<KeyType, ValueType>(id);
		sessions.put(id, session);
		return session;
	}

	public Transaction<KeyType, ValueType> addTransaction(Session<KeyType, ValueType> session, long id) {
		if (!sessions.containsKey(session.id) || transactions.containsKey(id)) {
			throw new InvalidHistoryError();
		}

		var txn = new Transaction<KeyType, ValueType>(id, session);
		transactions.put(id, txn);
		session.getTransactions().add(txn);
		return txn;
	}

	public Event<KeyType, ValueType> addEvent(Transaction<KeyType, ValueType> transaction, Event.EventType type, KeyType key,
			ValueType value) {
		var p = Pair.of(key, value);
		if (type == Event.EventType.WRITE) {
			if (!transactions.containsKey(transaction.id) || writes.contains(p)) {
				throw new InvalidHistoryError();
			}
			writes.add(p);
		}

		var ev = new Event<KeyType, ValueType>(transaction, type, key, value);
		transaction.getEvents().add(ev);
		return ev;
	}
}

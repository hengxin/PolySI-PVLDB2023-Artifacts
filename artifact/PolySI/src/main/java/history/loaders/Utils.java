package history.loaders;

import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import history.Event;
import history.History;

class Utils {
	static <T> HashMap<T, Long> getIdMap(Stream<T> keys, long beginId) {
		var f = new Function<T, Long>() {
			private long id = beginId;

			@Override
			public Long apply(T t) {
				return id++;
			}
		};

		var m = new HashMap<T, Long>();
		keys.forEach(k -> m.computeIfAbsent(k, f));
		return m;
	}

	static <T, U, V, W> History<T, U> convertHistory(History<V, W> history,
			Function<Event<V, W>, Pair<T, U>> keyValueConvert, Predicate<Event<V, W>> filter) {
		var sessions = history.getSessions();

		return new History<T, U>(
			sessions.stream().map(s -> s.getId()).collect(Collectors.toSet()),
			sessions.stream()
				.map(s -> Pair.of(s.getId(),
					s.getTransactions().stream()
						.map(t -> t.getId()).collect(Collectors.toList())))
				.collect(Collectors.toMap(Pair::getLeft, Pair::getRight)),
			history.getTransactions().stream()
				.map(t -> Pair.of(
					t.getId(),
					t.getEvents().stream().filter(filter).map(ev -> {
						var kv = keyValueConvert.apply(ev);
						return Triple.of(ev.getType(), kv.getLeft(), kv.getRight());
					}).collect(Collectors.toList())))
				.collect(Collectors.toMap(Pair::getLeft, Pair::getRight)));
	}
}

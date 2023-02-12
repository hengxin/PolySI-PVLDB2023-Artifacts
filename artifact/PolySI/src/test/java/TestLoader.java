import history.Event;
import history.History;
import history.HistoryLoader;
import lombok.AllArgsConstructor;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
public class TestLoader implements HistoryLoader<String, Integer> {
	final Set<Integer> sessions;
	final Map<Integer, List<Integer>> transactions;
	final Map<Integer, List<Triple<Event.EventType, String, Integer>>> events;

	@Override
	public History<String, Integer> loadHistory() {
		return new History<String, Integer>(
			sessions.stream().map(i -> (long)i).collect(Collectors.toSet()),
			transactions.entrySet().stream()
				.map(e -> Pair.of((long)e.getKey(),
					e.getValue().stream()
						.map(i -> (long)i).collect(Collectors.toList())))
				.collect(Collectors.toMap(Pair::getKey, Pair::getValue)),
			events.entrySet().stream()
				.map(e -> Pair.of((long)e.getKey(), e.getValue()))
				.collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
	}
}

package history;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Event<KeyType, ValueType> {
	public enum EventType {
    	READ, WRITE
    }

	@EqualsAndHashCode.Include
	private final Transaction<KeyType, ValueType> transaction;

	@EqualsAndHashCode.Include
	private final Event.EventType type;

	@EqualsAndHashCode.Include
	private final KeyType key;

	@EqualsAndHashCode.Include
	private final ValueType value;

	@Override
	public String toString() {
		return String.format("%s(%s, %s)", type, key, value);
	}
}

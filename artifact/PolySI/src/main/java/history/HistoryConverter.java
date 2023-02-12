package history;

public interface HistoryConverter<KeyType, ValueType> {
	<T, U> History<KeyType, ValueType> convertFrom(History<T, U> history);
}

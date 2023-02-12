package history;

public interface HistoryLoader<KeyType, ValueType> {
	History<KeyType, ValueType> loadHistory();
}

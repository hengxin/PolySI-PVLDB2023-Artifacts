package history;

public interface HistoryParser<KeyType, ValueType> extends HistoryConverter<KeyType, ValueType>,
		HistoryLoader<KeyType, ValueType>, HistoryDumper<KeyType, ValueType> {
}

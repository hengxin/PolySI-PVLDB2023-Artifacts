package history;

public interface HistoryTransformer
{
    public <T, U> History<?, ?> transformHistory(History<T, U> history);
}

package history.transformers;

import history.History;
import history.HistoryTransformer;

public class Identity implements HistoryTransformer {
    @Override
    public <T, U> History<T, U> transformHistory(History<T, U> history) {
        return history;
    }
}

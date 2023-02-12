package history;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Transaction<KeyType, ValueType> {
	public enum TransactionStatus {
    	ONGOING, COMMIT
    }

    @EqualsAndHashCode.Include
	final long id;

	@EqualsAndHashCode.Include
	private final Session<KeyType, ValueType> session;

	final List<Event<KeyType, ValueType>> events = new ArrayList<>();
	private Transaction.TransactionStatus status = Transaction.TransactionStatus.ONGOING;

    @Override
    public String toString() {
        return String.format("(%d, %d)", session.getId(), id);
    }
}

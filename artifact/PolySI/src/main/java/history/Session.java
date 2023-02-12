package history;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Session<KeyType, ValueType> {
	@EqualsAndHashCode.Include
	final long id;

	private final List<Transaction<KeyType, ValueType>> transactions = new ArrayList<>();
}

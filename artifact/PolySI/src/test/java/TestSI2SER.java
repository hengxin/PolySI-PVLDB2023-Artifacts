import history.Event;
import history.transformers.SnapshotIsolationToSerializable;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static history.Event.EventType.READ;
import static history.Event.EventType.WRITE;

public class TestSI2SER {
    @Test
    void lostUpdate() {
        var siHistory = new TestLoader(
            Set.of(1, 2),
            Map.of(
                1, List.of(1),
                2, List.of(2)
            ),
            Map.of(
                1, List.of(
                    Triple.of(READ, "x", 0),
                    Triple.of(WRITE, "x", 1)
                ),
                2, List.of(
                    Triple.of(READ, "x", 0),
                    Triple.of(WRITE, "x", 2)
                )
            )
        ).loadHistory();

        var serHistory = new SnapshotIsolationToSerializable().transformHistory(siHistory);
    }
}

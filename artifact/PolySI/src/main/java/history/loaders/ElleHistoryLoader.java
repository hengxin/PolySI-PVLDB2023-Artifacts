package history.loaders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Triple;

import history.History;
import history.HistoryLoader;
import history.Event.EventType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import static java.util.Map.entry;

@AllArgsConstructor
public class ElleHistoryLoader implements HistoryLoader<Integer, ElleHistoryLoader.ElleValue> {
    private final Path historyPath;

    @Override
    @SneakyThrows
    public History<Integer, ElleHistoryLoader.ElleValue> loadHistory() {
        try (var in = new BufferedReader(new FileReader(historyPath.toFile()))) {
            return parseFile(in);
        }
    }

    private History<Integer, ElleHistoryLoader.ElleValue> parseFile(BufferedReader reader) {
        var history = new History<Integer, ElleHistoryLoader.ElleValue>();
        reader.lines().forEachOrdered(line -> parseLine(history, CharBuffer.wrap(line)));

        var initSession = history.addSession(-1);
        var initTxn = history.addTransaction(initSession, -1);
        history.getEvents().stream().map(e -> e.getKey()).distinct()
                .forEach(k -> history.addEvent(initTxn, EventType.WRITE, k, new ElleValue(null, null)));

        return history;
    }

    private void parseLine(History<Integer, ElleHistoryLoader.ElleValue> history, CharBuffer line) {
        assertEq(line.charAt(0), '{');
        advance(line, 1);

        var keyRegex = Pattern.compile(":\\w+");
        ArrayList<Triple<EventType, Integer, ElleValue>> txnValue = null;
        Integer txnProcess = null;
        while (line.charAt(0) != '}') {
            var result = keyRegex.matcher(line.duplicate());

            if (!result.lookingAt()) {
                throw new RuntimeException(String.format("No match in \"%s\"", line));
            }
            skipCommaAndSpace(advance(line, result.group().length()));

            switch (result.group()) {
            case ":type":
                if (parseType(line) != LogType.OK) {
                    return;
                }
                break;
            case ":f":
                parseF(line);
                break;
            case ":value":
                txnValue = parseValue(line);
                break;
            case ":time":
                parseLong(line);
                break;
            case ":process":
                txnProcess = parseInt(line);
                break;
            case ":index":
                parseInt(line);
                break;
            default:
                throw new RuntimeException(String.format("Unknown key \"%s\"", result.group()));
            }

            skipCommaAndSpace(line);
        }

        if (txnProcess == null || txnValue == null) {
            throw new RuntimeException(String.format("Missing :process or :value in \"%s\"", line.clear()));
        }

        var session = history.getSession(txnProcess);
        if (session == null) {
            session = history.addSession(txnProcess);
        }

        var txnId = history.getTransactions().size();
        var txn = history.addTransaction(session, txnId);

        txnValue.forEach(v -> history.addEvent(txn, v.getLeft(), v.getMiddle(), v.getRight()));
    }

    private LogType parseType(CharBuffer s) {
        var typeMap = Map.ofEntries(
            entry(":invoke", LogType.INVOKE),
            entry(":ok", LogType.OK),
            entry(":fail", LogType.FAIL),
            entry(":info", LogType.INFO)
        );

        for (var e : typeMap.entrySet()) {
            if (startsWith(s, e.getKey())) {
                advance(s, e.getKey().length());
                return e.getValue();
            }
        }

        throw new RuntimeException(String.format("Unknown :type in \"%s\"", s));
    }

    private void parseF(CharBuffer s) {
        if (!startsWith(s, ":txn")) {
            throw new RuntimeException(String.format("Unknown :f in \"%s\"", s));
        }

        advance(s, ":txn".length());
    }

    private ArrayList<Triple<EventType, Integer, ElleValue>> parseValue(CharBuffer s) {
        assertEq(s.charAt(0), '[');
        advance(s, 1);

        var events = new ArrayList<Triple<EventType, Integer, ElleValue>>();
        while (true) {
            skipCommaAndSpace(s);
            if (s.charAt(0) == ']') {
                advance(s, 1);
                break;
            }
            events.add(parseEvent(s));
        }

        return events;
    }

    private Triple<EventType, Integer, ElleValue> parseEvent(CharBuffer s) {
        assertEq(s.charAt(0), '[');
        advance(s, 1);

        Triple<EventType, Integer, ElleValue> result;
        if (startsWith(s, ":r ")) {
            advance(s, ":r ".length());
            var key = parseInt(s);
            skipCommaAndSpace(s);
            var list = parseList(s);
            result = Triple.of(EventType.READ, key,
                    new ElleValue(list.isEmpty() ? null : list.get(list.size() - 1), list));
        } else if (startsWith(s, ":append ")) {
            advance(s, ":append ".length());
            var key = parseInt(s);
            skipCommaAndSpace(s);
            var value = parseInt(s);
            result = Triple.of(EventType.WRITE, key, new ElleValue(value, null));
        } else {
            throw new RuntimeException(String.format("Unknown event in \"%s\"", s));
        }

        assertEq(s.charAt(0), ']');
        advance(s, 1);
        return result;
    }

    private <T> T parseIntegerType(CharBuffer s, BiFunction<CharSequence, Integer, T> parser) {
        int i = 0;
        while (Character.isDigit(s.charAt(i))) {
            i++;
        }

        var result = parser.apply(s, i);
        advance(s, i);
        return result;
    }

    private Integer parseInt(CharBuffer s) {
        return parseIntegerType(s, (cs, i) -> Integer.parseInt(cs, 0, i, 10));
    }

    private Long parseLong(CharBuffer s) {
        return parseIntegerType(s, (cs, i) -> Long.parseLong(cs, 0, i, 10));
    }

    private List<Integer> parseList(CharBuffer s) {
        if (startsWith(s, "nil")) {
            advance(s, "nil".length());
            return List.of();
        }

        assertEq(s.charAt(0), '[');
        advance(s, 1);

        var list = new ArrayList<Integer>();
        while (true) {
            skipCommaAndSpace(s);
            if (s.charAt(0) == ']') {
                advance(s, 1);
                break;
            }
            list.add(parseInt(s));
        }

        return list;
    }

    private void assertEq(char a, char b) {
        if (a != b) {
            throw new AssertionError();
        }
    }

    private CharBuffer skipCommaAndSpace(CharBuffer s) {
        int i = 0;
        while (s.charAt(i) == ',' || s.charAt(i) == ' ') {
            i++;
        }

        return advance(s, i);
    }

    private CharBuffer advance(CharBuffer s, int length) {
        return s.position(s.position() + length);
    }

    private boolean startsWith(CharSequence a, CharSequence b) {
        for (int i = 0; i < b.length(); i++) {
            if (a.charAt(i) != b.charAt(i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * To map a list to PolySI's value, the last element in the list
     * is used for comparison and hashing. Elle guarantees that values
     * appended to the same list are unique.
     *
     * For a read-op, lastElement is the last element in the list read,
     * or null if the list is empty.
     *
     * For a write-op, lastElement is the appended value, and list is null.
     */
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    @AllArgsConstructor
    public static class ElleValue {
        @EqualsAndHashCode.Include
        Integer lastElement;

        List<Integer> list;

        @Override
        public String toString() {
            if (list == null) {
                return String.format("ElleAppend(%d)", lastElement);
            } else {
                return String.format("ElleList(%s)", list);
            }
        }
    }

    private static enum LogType {
        INVOKE, OK, FAIL, INFO
    }
}

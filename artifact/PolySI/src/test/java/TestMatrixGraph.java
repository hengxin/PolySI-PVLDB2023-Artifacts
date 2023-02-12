import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import graph.MatrixGraph;

class TestMatrixGraph {
    private static final int MATRIX_NODES = 1000;

    private Graph<Integer> generateGraph(int nodeNum, int edgesNum) {
        MutableGraph<Integer> graph = GraphBuilder.directed().build();

        IntStream.range(0, nodeNum).forEach(n -> graph.addNode(n));

        Streams.zip(new Random().ints(0, nodeNum).boxed(),
                new Random().ints(0, nodeNum).boxed(), Pair::of)
                .limit(edgesNum)
                .forEach(p -> {
                    if (!p.getLeft().equals(p.getRight())) {
                        graph.putEdge(p.getLeft(), p.getRight());
                    }
                });

        return graph;
    }

    @ParameterizedTest
    @ValueSource(doubles = { 5e-3 })
    void testComposition(double density) {
        var graph = generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density));
        var g = new MatrixGraph<>(graph);
        System.err.printf("density: %g\n", density);

        var t = Stopwatch.createStarted();
        var sparse = g.composition(g);
        System.err.printf("sparse: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var dense = g.composition(g);
        System.err.printf("dense: %s\n", t.elapsed());

        assertEquals(sparse, dense);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 5e-3 })
    void testReachability(double density) {
        var graph = generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density));
        var g = new MatrixGraph<>(graph);
        System.err.printf("density: %g\n", density);

        var t = Stopwatch.createStarted();
        var sparse = g.reachability();
        System.err.printf("sparse: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var dense = g.reachability();
        System.err.printf("dense: %s\n", t.elapsed());

        assertEquals(sparse, dense);
    }

    @Test
    void testTopoSort() {
        var graph = new MatrixGraph<Integer>(
                GraphBuilder.directed().<Integer>immutable()
                        .addNode(1)
                        .addNode(2)
                        .addNode(3)
                        .putEdge(1, 2)
                        .putEdge(2, 3)
                        .build());

        assertEquals(graph.topologicalSort(), Optional.of(List.of(1, 2, 3)));
    }
}

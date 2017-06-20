package org.graylog.jmh.jest;

import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Collections;

public class JestIndexBenchmark extends AbstractBenchmark {
    @State(Scope.Thread)
    public static class BenchmarkState extends JestClientState {
        @Override
        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            super.doSetup();
        }

        @Override
        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            super.doTearDown();
        }
    }

    @Benchmark
    public void indexSingle(BenchmarkState state, Blackhole blackhole) throws IOException {
        final JestClient jestClient = state.jestClient;
        final Index index = new Index.Builder(Collections.singletonMap("test", "foobar"))
                .index(state.indexName)
                .type("benchmark")
                .build();
        final DocumentResult result = jestClient.execute(index);
        blackhole.consume(result);
    }

    @Benchmark
    public void indexSingleGzip(BenchmarkState state, Blackhole blackhole) throws IOException {
        final JestClient jestClient = state.jestClientGzip;
        final Index index = new Index.Builder(Collections.singletonMap("test", "foobar"))
                .index(state.indexName)
                .type("benchmark")
                .build();
        final DocumentResult result = jestClient.execute(index);
        blackhole.consume(result);
    }
}

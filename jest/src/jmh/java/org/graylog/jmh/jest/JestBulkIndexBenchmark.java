package org.graylog.jmh.jest;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Collections;

public class JestBulkIndexBenchmark extends AbstractBenchmark {

    @State(Scope.Thread)
    public static class BulkState extends JestClientState {
        Bulk bulk;

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            super.doSetup();
            final Index index = new Index.Builder(Collections.singletonMap("test", "foobar"))
                    .index(indexName)
                    .type("benchmark")
                    .build();
            final Bulk.Builder bulkBuilder = new Bulk.Builder();

            for (int i = 0; i < 100_000; i++) {
                bulkBuilder.addAction(index);
            }

            bulk = bulkBuilder.build();
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            if (bulk != null) {
                bulk = null;
            }
            super.doTearDown();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void indexMany(BulkState state, Blackhole blackhole) throws IOException {
        final JestClient jestClient = state.jestClient;
        final BulkResult bulkResult = jestClient.execute(state.bulk);
        blackhole.consume(bulkResult);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void indexManyGzip(BulkState state, Blackhole blackhole) throws IOException {
        final JestClient jestClient = state.jestClientGzip;
        final BulkResult bulkResult = jestClient.execute(state.bulk);
        blackhole.consume(bulkResult);
    }
}

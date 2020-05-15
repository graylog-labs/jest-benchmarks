package org.graylog.jmh.elastic;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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

public class ElasticBulkIndexBenchmark extends AbstractBenchmark {

    @State(Scope.Thread)
    public static class BulkState extends ElasticClientState {
        BulkRequest bulk;

        @Setup(Level.Trial)
        public void doSetup() throws IOException {
            super.doSetup();
            this.bulk = new BulkRequest();
            final IndexRequest index = new IndexRequest(indexName, this.type).source(Collections.singletonMap("test", "foobar"));
            for (int i = 0; i < 100_000; i++) {
                bulk.add(index);
            }
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
        final RestHighLevelClient restClient = state.restClient;
        final BulkResponse bulkResult = restClient.bulk(state.bulk, RequestOptions.DEFAULT);
        blackhole.consume(bulkResult);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void indexManyGzip(BulkState state, Blackhole blackhole) throws IOException {
        final RequestOptions.Builder requestOptions = RequestOptions.DEFAULT
                .toBuilder();
        requestOptions.addHeader("Accept-Encoding", "gzip,deflate");
        final RestHighLevelClient restClient = state.restClient;
        final BulkResponse bulkResult = restClient.bulk(state.bulk, requestOptions.build());
        blackhole.consume(bulkResult);
    }
}

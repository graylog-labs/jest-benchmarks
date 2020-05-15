package org.graylog.jmh.elastic;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Collections;

public class ElasticIndexBenchmark extends AbstractBenchmark {
    @State(Scope.Thread)
    public static class BenchmarkState extends ElasticClientState {
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
        final RestHighLevelClient restClient = state.restClient;
        final IndexRequest index = new IndexRequest(state.indexName, state.type).source(Collections.singletonMap("test", "foobar"));
        final IndexResponse result = restClient.index(index, RequestOptions.DEFAULT);
        blackhole.consume(result);
    }

    /*@Benchmark
    public void indexSingleGzip(BenchmarkState state, Blackhole blackhole) throws IOException {
        final RestHighLevelClient restClient = state.restClient;
        final IndexRequest index = new IndexRequest(state.indexName, state.type).source(Collections.singletonMap("test", "foobar"));
        final RequestOptions.Builder requestOptions = RequestOptions.DEFAULT
                .toBuilder();
        requestOptions.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");
        final IndexResponse result = restClient.index(index, requestOptions.build());
        blackhole.consume(result);
    }*/
}

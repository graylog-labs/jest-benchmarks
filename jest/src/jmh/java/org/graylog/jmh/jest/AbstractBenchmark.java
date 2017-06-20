package org.graylog.jmh.jest;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;

public abstract class AbstractBenchmark {
    protected static final String ES_HOST = System.getProperty("es.host", "127.0.0.1");
    protected static final int ES_PORT = Integer.getInteger("es.port", 9200);


    public static class JestClientState {
        public final String indexName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, this.getClass().getSimpleName());
        public JestClient jestClient;
        public JestClient jestClientGzip;

        public void doSetup() throws IOException {
            final Gson gson = new Gson();
            final HttpClientConfig.Builder configBuilder = new HttpClientConfig.Builder("http://" + ES_HOST + ":" + ES_PORT)
                    .gson(gson);
            final HttpClientConfig httpClientConfig = configBuilder
                    .requestCompressionEnabled(false)
                    .readTimeout(10_000)
                    .build();
            final HttpClientConfig httpClientConfigGzip = configBuilder
                    .requestCompressionEnabled(true)
                    .readTimeout(10_000)
                    .build();

            final JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(httpClientConfig);
            jestClient = factory.getObject();

            final JestClientFactory factoryGzip = new JestClientFactory();
            factoryGzip.setHttpClientConfig(httpClientConfigGzip);
            jestClientGzip = factory.getObject();

            if (indicesExists(jestClient, indexName)) {
                deleteIndex(jestClient, indexName);
            }
            createIndex(jestClient, indexName);
        }

        public void doTearDown() throws IOException {
            if (jestClient != null) {
                if (indicesExists(jestClient, indexName)) {
                    deleteIndex(jestClient, indexName);
                }

                jestClient.shutdownClient();
            }
            if (jestClientGzip != null) {
                jestClientGzip.shutdownClient();
            }
        }

        private void createIndex(JestClient jestClient, String index) throws IOException {
            final ImmutableMap<String, Integer> settings = ImmutableMap.of(
                    "number_of_shards", 1,
                    "number_of_replicas", 0);
            final CreateIndex createIndex = new CreateIndex.Builder(index)
                    .settings(settings)
                    .build();
            final JestResult jestResult = jestClient.execute(createIndex);
            Preconditions.checkState(jestResult.isSucceeded(), jestResult.getErrorMessage());
        }

        private void deleteIndex(JestClient jestClient, String index) throws IOException {
            final DeleteIndex deleteIndex = new DeleteIndex.Builder(index).build();
            final JestResult jestResult = jestClient.execute(deleteIndex);
            Preconditions.checkState(jestResult.isSucceeded(), jestResult.getErrorMessage());
        }

        private boolean indicesExists(JestClient jestClient, String index) throws IOException {
            final IndicesExists indicesExists = new IndicesExists.Builder(index).build();
            final JestResult jestResult = jestClient.execute(indicesExists);
            return jestResult.isSucceeded();
        }
    }


}

package org.graylog.jmh.elastic;

import com.google.common.base.CaseFormat;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public abstract class AbstractBenchmark {
    protected static final String ES_HOST = System.getProperty("es.host", "127.0.0.1");
    protected static final int ES_PORT = Integer.getInteger("es.port", 9200);


    public static class ElasticClientState {
        public final String indexName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, this.getClass().getSimpleName());
        public RestHighLevelClient restClient;
        public String type = "benchmark";

        public void doSetup() throws IOException {
            restClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(ES_HOST, ES_PORT, "http")));

            if (indicesExists(restClient, indexName)) {
                deleteIndex(restClient, indexName);
            }
            createIndex(restClient, indexName);
        }

        public void doTearDown() throws IOException {
            if (restClient != null) {
                if (indicesExists(restClient, indexName)) {
                    deleteIndex(restClient, indexName);
                }

                restClient.close();
            }
        }

        private void createIndex(RestHighLevelClient restClient, String index) throws IOException {
            CreateIndexRequest request = new CreateIndexRequest(index);
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            );

            restClient.indices().create(request, RequestOptions.DEFAULT);
        }

        private void deleteIndex(RestHighLevelClient restClient, String index) throws IOException {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            restClient.indices().delete(request, RequestOptions.DEFAULT);
        }

        private boolean indicesExists(RestHighLevelClient restClient, String index) throws IOException {
            GetIndexRequest indicesExistsRequest = new GetIndexRequest(index);
            return restClient.indices().exists(indicesExistsRequest, RequestOptions.DEFAULT);
        }
    }


}

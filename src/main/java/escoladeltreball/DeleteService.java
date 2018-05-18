package escoladeltreball;


import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

/***
 * add delete by query plugin to Elastisearch
 * $ bin/plugin install delete-by-query
 */
public class DeleteService {


    Client client;

    public DeleteService(Client client) {
        this.client = client;
    }


    public String delete(String id) {

        return client.prepareDelete("library", "book", id).get().getResult().toString();
    }

    public long deleteByQuery(String name) {

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", name))
                .source("twitter")
                .get();
        long deleted = response.getDeleted();
        return deleted;
    }
}
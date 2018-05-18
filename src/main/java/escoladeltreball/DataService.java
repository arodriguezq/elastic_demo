package escoladeltreball;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

public class DataService {
    Client client;

    public DataService(Client client) {
        this.client = client;
    }

    public List<String> queryOne() {
        QueryBuilder query = matchQuery("text", "tweet twitter").analyzer("english");
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> queryTwo() {
        QueryBuilder query = multiMatchQuery("cia", "references").analyzer("english");
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> queryThree() {
        MultiMatchQueryBuilder query
                = new MultiMatchQueryBuilder("gobernment").field("text").fuzziness("AUTO");
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> queryFour() {
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder("cia").field("text",5).field("references",1).analyzer("english");
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            list.add(hit.getSourceAsString());
        }
        return list;
    }
}
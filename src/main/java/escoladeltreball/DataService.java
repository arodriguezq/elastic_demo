package escoladeltreball;

import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class DataService {


    Client client;

    public DataService(Client client) {
        this.client = client;
    }

    public List<String> queryOne() {
        QueryBuilder query = matchQuery("title", "tweet twitter").analyzer("english");
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> queryTwo() {
        QueryBuilder query = multiMatchQuery("references", "cia").analyzer("english");
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> queryThree() {
        MultiMatchQueryBuilder query
                = new MultiMatchQueryBuilder("text","gobernment").fuzziness("AUTO").analyzer("english");
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
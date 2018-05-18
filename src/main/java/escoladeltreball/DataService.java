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

    public List<String> getMatchAllQueryData() {
        QueryBuilder query = matchAllQuery();
        System.out.println("getMatchAllQueryCount query =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getBoolQueryData() {
        QueryBuilder query = boolQuery().must(
                termQuery("user", "skyji")
        ).must(termQuery("location", "India"));
        System.out.println("getBoolQueryCount query =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getPhraseQueryData() {
        QueryBuilder query = matchPhraseQuery("name", "samuel");
        System.out.println("getPhraseQueryCount query =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> queryOne() {
        QueryBuilder query = matchQuery("title", "tweet twitter").analyzer("english");
//        System.out.println("Query 1 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("twitter").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getQueryData2() {
        QueryBuilder query = multiMatchQuery("elasticsearch guide", "summary","title");
        System.out.println("Query 2 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getQueryData3() {


        MultiMatchQueryBuilder query
                = new MultiMatchQueryBuilder("elasticsearch guide").field("summary",3).field("title",1);

        System.out.println("Query 3 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getQueryData4() {


        BoolQueryBuilder query= boolQuery()
                .must(matchQuery("authors", "clinton gormely"))
                .should(multiMatchQuery("title", "Elasticsearch", "Solr"))
                .mustNot(matchQuery("authors", "radu gheorge"));


        System.out.println("Query 4 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getQueryData5() {


        MultiMatchQueryBuilder query
                = new MultiMatchQueryBuilder("comprihensiv guide").field("summary",1).field("title",1).fuzziness("AUTO");


        System.out.println("Query 5 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getQueryData6() {


        WildcardQueryBuilder query
                = wildcardQuery(
                "authors",
                "t*");


        System.out.println("Query 6 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }

    public List<String> getQueryData7() {


        MultiMatchQueryBuilder query
                = new MultiMatchQueryBuilder("search engine").field("summary",1).field("title",1).type("phrase").slop(3);

        System.out.println("Query 6 =>" + query.toString());
        SearchHit[] hits = client.prepareSearch("library").setQuery(query).execute().actionGet().getHits().getHits();
        List<String> list = new ArrayList<String>();
        for (SearchHit hit : hits) {
            // hit.sourceAsMap()
            list.add(hit.getSourceAsString());
        }
        return list;
    }


}
package escoladeltreball;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class App {


    public static void main(String[] args) throws IOException {
        App app = new App();

        ESManager esManager = new ESManager();
        Client client = esManager.getClient("192.168.2.41", 9200).get();

        DataService dataService = new DataService(client);
        DeleteService deleteService = new DeleteService(client);

        //app.deleteIndex(client);
        app.initialInserts(client);
        // Buscar libros que contengan “in Action”  en su título
        System.out.println("#=> Busca los tweets que contengan la palabra \"tweet\" o \"twitter\".");
        dataService.queryOne().forEach(item -> System.out.println(item));
        // Buscar libros que contengan “elasticsearch guide”  en su título o descripcion
        //dataService.getQueryData2().forEach(item -> System.out.println(item));
        // Buscar libros que contengan “elasticsearch guide”  en su título o descripcion, pero dandole mas importancio a los que lo tengan en la descripcion
        //dataService.getQueryData3().forEach(item -> System.out.println(item));
        //Busca libros que contengan "Elasticsearch" o "Solr" en el título y que su autor sea "clinton gormley" y no "radu gheorge"
        //dataService.getQueryData4().forEach(item -> System.out.println(item));
        //Buscar libros que contengan "comprihensiv guide" paara comprobar fuzzines
        //dataService.getQueryData5().forEach(item -> System.out.println(item));
        //Buscar los libros cuyos autores comienzen por la letra t
        //dataService.getQueryData6().forEach(item -> System.out.println(item));
        //Buscar los libros que contengan las palabras "search engine" en su título o descripción juntas o como mucho a una distancia de 3
        //dataService.getQueryData7().forEach(item -> System.out.println(item));

   /*     // Data
        System.out.println("\ngetMatchAllQueryData from ES::: ");
        dataService.getMatchAllQueryData().forEach(item -> System.out.println(item));*/
      /*  System.out.println("\ngetBoolQueryData from ES::: ");
        dataService.getBoolQueryData().forEach(item -> System.out.println(item));



        System.out.println("\ngetPhraseQueryData from ES::: ");
        dataService.getPhraseQueryData().forEach(item -> System.out.println(item));


        // Delete
        // delete one record by id
        //System.out.println("delete by id " + deleteService.delete("AVSMh1LBWlqOklhqtVNs"));
        //delete record by query
        System.out.println("delete by query " + deleteService.deleteByQuery("samuel"));*/

        client.close();

    }

    private void deleteIndex(Client client) {
        DeleteIndexResponse deleteResponse = client.admin().indices().delete(new DeleteIndexRequest("library")).actionGet();
    }


    public void initialInserts(Client client) throws IOException {
        client.prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("text", "OMG my first tweet!")
                        .field("user", "aitor")
                        .field("publish_date", "2018-05-18")
                        .field("likes", "20")
                        .field("references", "none")
                        .endObject()
                ).get();

        client.prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("text", "@sarah just setting up my twitter. It's very easy.")
                        .field("user", "aitor")
                        .field("publish_date", "2018-05-18")
                        .field("likes", "5")
                        .field("references", "sara")
                        .endObject()
                ).get();

        client.prepareIndex("twitter", "tweet", "3")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("text", "Fantastic! The next step is to criticize everything for no reason")
                        .field("user", "sarah")
                        .field("publish_date", "2018-05-18")
                        .field("likes", "20")
                        .field("references", "aitor")
                        .endObject()
                ).get();

        client.prepareIndex("twitter", "tweet", "4")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("text", "hum... It seems funny. I try it")
                        .field("user", "aitor")
                        .field("publish_date", "2018-05-18")
                        .field("likes", "20")
                        .field("references", "none")
                        .endObject()
                ).get();

        client.prepareIndex("twitter", "tweet", "5")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("text", "¡La CIA nos está espiando a todos! #STOP @CIA")
                        .field("user", "aitor")
                        .field("publish_date", "2018-05-18")
                        .field("likes", "20")
                        .field("references", "cia")
                        .endObject()
                ).get();

        client.prepareIndex("twitter", "tweet", "6")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("text", "We proceed to close your account. Att. USA government @aitor")
                        .field("user", "cia")
                        .field("publish_date", "2018-05-19")
                        .field("likes", "20")
                        .field("references", "aitor")
                        .endObject()
                ).get();
    }
}
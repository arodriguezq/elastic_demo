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
        Client client = esManager.getClient("localhost", 9300).get();

        DataService dataService = new DataService(client);
        DeleteService deleteService = new DeleteService(client);

        //app.deleteIndex(client);
        app.initialInserts(client);
        System.out.println("\n\n#=> Busca los tweets que contengan la palabra \"tweet\" o \"twitter\".");
        dataService.queryOne().forEach(item -> System.out.println(item));
        System.out.println("\n\n#=> Busca los tweets con referencia a la CIA");
        dataService.queryTwo().forEach(item -> System.out.println(item));
        System.out.println("\n\n#=> Busca los tweets que contengan la palabra gobernment (mal escrita)");
        dataService.queryThree().forEach(item -> System.out.println(item));
        System.out.println("\n\n#=> Busca los tweets que contengan la cia tanto en referencia como en texto dándole a este último más relevancia");
        dataService.queryFour().forEach(item -> System.out.println(item));

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
                        .field("references", "aitor cia")
                        .endObject()
                ).get();
    }
}
/**
 * Hugo COLLIN - 05/11/2023
 */

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.util.List;

/**
 * Classe utilitaire pour l'affichage des données.
 */
public class DisplayData {
    /**
     * Affiche le résultat de la requête.
     *
     * @param q          Question
     * @param collection Collection des équipes
     * @param pipeline   Pipeline de la requête
     */
    static void displayQuestionJson(String q, MongoCollection<Document> collection, List<Bson> pipeline) {
        sepQ5(q);
        displayResult(collection, pipeline);
        sep();
    }

    /**
     * Affiche le résultat de la requête.
     *
     * @param collection Collection des équipes
     * @param pipeline   Pipeline de la requête
     */
    static void displayResult(MongoCollection<Document> collection, List<Bson> pipeline) {
        for (Document doc : collection.aggregate(pipeline)) {
            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));
        }
    }

    /**
     * Affiche la séparation entre les questions dans le terminal.
     *
     * @param q Numéro de question
     */
    static void sepQ5(String q) {
        System.out.println("--- Q5." + q + " ---");
    }

    /**
     * Affiche un séparateur dans le terminal.
     */
    static void sep() {
        System.out.println("---");
    }
}
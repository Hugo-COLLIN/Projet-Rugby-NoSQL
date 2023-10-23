/**
 * Hugo COLLIN - /10/2023
 */

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.*;
import org.bson.conversions.Bson;
import org.bson.types.*;
import com.mongodb.client.*;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

public class JavaMongoConnection {
    public static void main(String[] args) {
        String uri = "mongodb://localhost:27017";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("ProjetRugby");
            MongoCollection<Document> collection = sampleTrainingDB.getCollection("equipes");


            //Q5.a
            System.out.println("--- Q5.a ---");
            String equipeE = "ENG"; // Code de l'équipe E

            List<Bson> pipeline = Arrays.asList(
                    Aggregates.match(Filters.eq("codeEquipe", equipeE)), // Filtrer pour l'équipe E
                    Aggregates.unwind("$matchs"), // Déconstruire les matchs
                    Aggregates.unwind("$matchs.performances"), // Déconstruire les performances
                    Aggregates.group("$matchs.performances.numeroJoueur", // Grouper par numéro de joueur
                            sum("totalTempsDeJeu", "$matchs.performances.tempsDeJeu"),
                            sum("totalEssais", "$matchs.performances.essaisMarques"),
                            sum("totalPoints", "$matchs.performances.pointsMarques")
                    ),
                    Aggregates.project(fields(
                            include("_id", "totalTempsDeJeu", "totalEssais", "totalPoints"),
                            computed("coefficient", new Document("$divide", Arrays.asList("$totalPoints", "$totalTempsDeJeu"))) // Calculer le coefficient
                    )),
                    Aggregates.sort(descending("coefficient")) // Trier par ordre décroissant du coefficient
            );


            for (Document doc : collection.aggregate(pipeline)) {
                System.out.println(doc.toJson());
            }

            mongoClient.close();
        }
    }
}
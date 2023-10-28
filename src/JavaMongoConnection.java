/**
 * Hugo COLLIN - /10/2023
 */

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.*;
import org.bson.conversions.Bson;
import com.mongodb.client.*;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

public class JavaMongoConnection {
    public static void main(String[] args) {
        String uri = "mongodb://localhost:27017";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("ProjetRugby");
            MongoCollection<Document> collection = sampleTrainingDB.getCollection("equipes");


            q5a(collection, "ENG");
            q5b(collection, "2023-09-22", 10);
            q5c(collection, "Arbitre A");
//            q5d(collection);
//            q5e(collection);
//            q5f(collection);
//            q5g(collection);
//            q5h(collection);
//            q5i(collection);
//            q5j(collection);
//            q5k(collection);



            mongoClient.close();
        }
    }

    private static void q5c(MongoCollection<Document> collection, String arbitreA) {
        q5sep("c");
        Bson filter = Filters.and(
                Filters.exists("matchs.equipeRecevant"),
                Filters.eq("matchs.arbitre.nom", arbitreA)
        );
        try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
        sep();
    }

    private static void q5b(MongoCollection<Document> collection, String dateD, int pointsP) {
        q5sep("b");

        Bson filter = Filters.and(
                Filters.eq("matchs.date", dateD),
                Filters.gt("matchs.nombrePoints", pointsP)
        );
        try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
        sep();
    }

    private static void q5a(MongoCollection<Document> collection, String equipeE) {
        q5sep("a");

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

        sep();
    }

    static void q5sep(String q) {
        System.out.println("--- Q5." + q + " ---");
    }

    static void sep() {
        System.out.println("---");
    }
}
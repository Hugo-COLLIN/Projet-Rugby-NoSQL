/**
 * Hugo COLLIN - /10/2023
 */

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.*;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import org.bson.json.JsonWriterSettings;

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
            q5c(collection, "Barnes");
//            q5d(collection, "2023-09-22", "ENG", "ESP");
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

//    private static void q5d(MongoCollection<Document> collection, String dateD, String equipeE1, String equipeE2) {
//        q5sep("d");
//
//        List<Bson> pipeline = Arrays.asList(
//                Aggregates.match(Filters.eq("codeEquipe", equipeE1)), // Filtrer pour l'équipe E1
//                Aggregates.unwind("$joueurs"), // Déconstruire les joueurs
//                Aggregates.unwind("$matchs"), // Déconstruire les matchs
//                Aggregates.match(
//                        Filters.and(
//                                Filters.eq("matchs.date", dateD),
//                                Filters.or(
//                                        Filters.eq("matchs.equipeRecevant", equipeE2),
//                                        Filters.eq("matchs.equipeReçue", equipeE2)
//                                ),
//                                Filters.eq("matchs.performances.debutMatch", true),
//                                Filters.eq("matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")
//                        )
//                ), // Filtrer par date, équipe E2, début du match et numéro de joueur
//                Aggregates.project(
//                        fields(
//                                excludeId(),
//                                computed("nom", "$joueurs.nom"),
//                                computed("prenom", "$joueurs.prenom")
//                        )
//                ) // Inclure uniquement le nom et le prénom du joueur
//        );
//
//        displayResult(collection, pipeline);
//
//        sep();
//    }

    private static void q5c(MongoCollection<Document> collection, String arbitreA) {
        q5sep("c");

        List<Bson> pipeline = Arrays.asList(
                Aggregates.unwind("$matchs"), // Déconstruire les matchs
                Aggregates.match(
                        Filters.and(
                                Filters.exists("matchs.equipeRecevant"),
                                Filters.eq("matchs.arbitre.nom", arbitreA)
                        )
                ), // Filtrer par équipe recevant et nom de l'arbitre
                Aggregates.project(
                        fields(
                                excludeId(),
                                computed("nom", "$matchs.arbitre.nom"),
                                computed("prenom", "$matchs.arbitre.prenom"),
                                computed("nationalite", "$matchs.arbitre.nationalite")
                        )
                ) // Inclure uniquement l'objet arbitre
        );

        displayResult(collection, pipeline);

        sep();
    }

    private static void q5b(MongoCollection<Document> collection, String dateD, int pointsP) {
        q5sep("b");

        List<Bson> pipeline = Arrays.asList(
                Aggregates.unwind("$matchs"), // Déconstruire les matchs
                Aggregates.match(
                        Filters.and(
                                Filters.eq("matchs.date", dateD),
                                Filters.gt("matchs.nombrePoints", pointsP)
                        )
                ), // Filtrer par date et nombre de points
                Aggregates.project(
                        fields(
                                excludeId(),
                                computed("numeroMatch", "$matchs.numeroMatch"),
                                computed("date", "$matchs.date"),
                                computed("evenement", "$matchs.evenement"),
                                computed("stade", "$matchs.stade"),
                                computed("equipeRecevant", "$matchs.equipeRecevant"),
                                computed("equipeReçue", "$matchs.equipeReçue"),
                                computed("nombrePoints", "$matchs.nombrePoints"),
                                computed("nombreEssais", "$matchs.nombreEssais"),
                                computed("arbitre", "$matchs.arbitre"),
                                computed("nombreSpectateurs", "$matchs.nombreSpectateurs"),
                                computed("performances", "$matchs.performances")
                        )
                ) // Inclure uniquement l'objet match
        );

        displayResult(collection, pipeline);

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


        displayResult(collection, pipeline);

        sep();
    }

    private static void displayResult(MongoCollection<Document> collection, List<Bson> pipeline) {
        for (Document doc : collection.aggregate(pipeline)) {
            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));
        }
    }

    static void q5sep(String q) {
        System.out.println("--- Q5." + q + " ---");
    }

    static void sep() {
        System.out.println("---");
    }
}
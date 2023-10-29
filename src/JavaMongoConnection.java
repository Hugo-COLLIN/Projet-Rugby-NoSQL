/**
 * Hugo COLLIN - /10/2023
 */

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.*;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import org.bson.json.JsonWriterSettings;

import java.util.*;

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


//            q5a(collection, "ENG");
//            q5b(collection, "2023-09-22", 10);
//            q5c(collection, "Barnes");
//            q5d(collection, "2023-09-25", "ENG", "ESP");
//            q5e(collection, "ENG");
            q5f(collection, "ENG");
            q5g(collection, "ENG", "ESP", "FRA");
//            q5h(collection);
//            q5i(collection);
//            q5j(collection);
//            q5k(collection);



            mongoClient.close();
        }
    }

    private static void q5g(MongoCollection<Document> collection, String equipeE1, String equipeE2, String equipeE3) {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq("codeEquipe", equipeE1)),
                Aggregates.unwind("$joueurs"),
                Aggregates.unwind("$matchs"),
                Aggregates.unwind("$matchs.performances"),
                Aggregates.match(
                        Filters.and(
                                Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur"))),
                                Filters.or(
                                        Filters.eq("matchs.equipeRecevant", equipeE2),
                                        Filters.eq("matchs.equipeRecevant", equipeE3),
                                        Filters.eq("matchs.equipeReçue", equipeE2),
                                        Filters.eq("matchs.equipeReçue", equipeE3)
                                )
                        )
                ),
                Aggregates.group("$joueurs.numeroJoueur",
                        Accumulators.first("nom", "$joueurs.nom"),
                        Accumulators.first("prenom", "$joueurs.prenom")
                ),
                aggregatesProjectIncludeFields("nom", "prenom")

        );

        displayQuestionJson("g", collection, pipeline);
    }


    private static void q5f(MongoCollection<Document> collection, String equipeE) {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq("codeEquipe", equipeE)), // Filtrer pour l'équipe E
                Aggregates.unwind("$joueurs"), // Déconstruire les joueurs
                Aggregates.unwind("$matchs"), // Déconstruire les matchs
                Aggregates.unwind("$matchs.performances"), // Déconstruire les performances
                Aggregates.match(
                        Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))
                ), // Filtrer par numéro de joueur
                Aggregates.group("$joueurs.numeroJoueur", // Grouper par numéro de joueur
                        Accumulators.first("nom", "$joueurs.nom"),
                        Accumulators.first("prenom", "$joueurs.prenom"),
                        Accumulators.sum("totalMatchs", 1),
                        Accumulators.sum("totalEssais", "$matchs.performances.essaisMarques"),
                        Accumulators.sum("totalPoints", "$matchs.performances.pointsMarques")
                ), // Aggréger les valeurs
                aggregatesProjectIncludeFields("nom", "prenom", "totalMatchs", "totalEssais", "totalPoints") // Inclure uniquement les champs nécessaires

        );

        displayQuestionJson("f", collection, pipeline);
    }


    private static void q5e(MongoCollection<Document> collection, String equipeE) {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq("codeEquipe", equipeE)), // Filtrer pour l'équipe E
                Aggregates.unwind("$joueurs"), // Déconstruire les joueurs
                Aggregates.unwind("$matchs"), // Déconstruire les matchs
                Aggregates.unwind("$matchs.performances"), // Déconstruire les performances
                Aggregates.match(
                        Filters.and(
                                Filters.eq("matchs.performances.debutMatch", false),
                                Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))
                        )
                ), // Filtrer par début du match et numéro de joueur
                aggregatesProjectIncludeFields("joueurs.nom", "joueurs.prenom") // Inclure uniquement le nom et le prénom du joueur
        );

        displayQuestionJson("e", collection, pipeline);
    }

    private static void q5d(MongoCollection<Document> collection, String dateD, String equipeE1, String equipeE2) {
        List<Bson> pipeline = Arrays.asList(
            Aggregates.match(Filters.eq("codeEquipe", equipeE1)), // Filtrer pour l'équipe E1
            Aggregates.unwind("$joueurs"), // Déconstruire les joueurs
            Aggregates.unwind("$matchs"), // Déconstruire les matchs
            Aggregates.unwind("$matchs.performances"), // Déconstruire les performances
            Aggregates.match(
                    Filters.and(
                            Filters.eq("matchs.date", dateD),
                            Filters.or(
                                    Filters.eq("matchs.equipeRecevant", equipeE2),
                                    Filters.eq("matchs.equipeReçue", equipeE2)
                            ),
                            Filters.eq("matchs.performances.debutMatch", true),
                            Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))
                    )
            ), // Filtrer par date, équipe E2, début du match et numéro de joueur
            aggregatesProjectIncludeFields("joueurs.nom", "joueurs.prenom") // Inclure uniquement le nom et le prénom du joueur
        );

        displayQuestionJson("d", collection, pipeline);
    }

    private static void q5c(MongoCollection<Document> collection, String arbitreA) {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.unwind("$matchs"), // Déconstruire les matchs
                Aggregates.match(
                        Filters.and(
                                Filters.exists("matchs.equipeRecevant"),
                                Filters.eq("matchs.arbitre.nom", arbitreA)
                        )
                ), // Filtrer par équipe recevant et nom de l'arbitre
                // Inclure uniquement l'objet arbitre
                aggregatesProjectIncludeFields("matchs.arbitre.nom", "matchs.arbitre.prenom", "matchs.arbitre.nationalite")
        );

        displayQuestionJson("c", collection, pipeline);
    }

    private static void q5b(MongoCollection<Document> collection, String dateD, int pointsP) {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.unwind("$matchs"), // Déconstruire les matchs
                Aggregates.match(
                        Filters.and(
                                Filters.eq("matchs.date", dateD),
                                Filters.gt("matchs.nombrePoints", pointsP)
                        )
                ), // Filtrer par date et nombre de points
                // Inclure uniquement les champs de l'objet match
                aggregatesProjectIncludeFields("matchs.numeroMatch", "matchs.date", "matchs.evenement",
                                        "matchs.stade", "matchs.equipeRecevant", "matchs.equipeReçue",
                                        "matchs.nombrePoints", "matchs.nombreEssais", "matchs.arbitre",
                                        "matchs.nombreSpectateurs", "matchs.performances")
        );

        displayQuestionJson("b", collection, pipeline);
    }

    private static void q5a(MongoCollection<Document> collection, String equipeE) {
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


        displayQuestionJson("a", collection, pipeline);
    }

    private static Bson aggregatesProjectIncludeFields(String ...fieldsNames) {
        return Aggregates.project(
                fields(
                        excludeId(),
                        include(fieldsNames)
                )
        );
    }

    private static void displayQuestionJson(String q, MongoCollection<Document> collection, List<Bson> pipeline) {
        q5sep(q);
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
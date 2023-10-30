/**
 * Hugo COLLIN - /10/2023
 */

import com.mongodb.client.model.*;
import org.bson.*;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

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
            Scanner scanner = new Scanner(System.in);
            String question;
            boolean quit = false;

            while (!quit) {
                System.out.print("Choisissez une question (a, b, c, d, e, f, g, h, i, j, k) ou écrivez quit pour quitter: ");
                question = scanner.nextLine();

                switch (question) {
                    case "a":
//                        System.out.print("Entrez le code de l'équipe (ENG): ");
//                        String codeEquipe = scanner.nextLine();
//                        if (codeEquipe.isEmpty())
//                            codeEquipe = "ENG";
//                        if (codeEquipe.length() != 3) {
//                            System.out.println("Le code de l'équipe doit être composé de 3 lettres.");
//                            break;
//                        }
//                        q5a(collection, codeEquipe);
                        q5a(collection, "ENG");
                        break;
                    case "b":
                        q5b(collection, "2023-09-22", 10);
                        break;
                    case "c":
                        q5c(collection, "Barnes");
                        break;
                    case "d":
                        q5d(collection, "2023-09-25", "ENG", "ESP");
                        break;
                    case "e":
                        q5e(collection, "ENG");
                        break;
                    case "f":
                        q5f(collection, "ENG");
                        break;
                    case "g":
                        q5g(collection, "ENG", "ESP", "FRA");
                        break;
                    case "h":
                        q5h(collection, "ENG");
                        break;
                    case "i":
                        q5i(collection, "ENG");
                        break;
                    case "j":
                        q5j(collection);
                        break;
                    case "k":
                        q5k(collection, 1, new Document("nom", "Clat").append("prenom", "Cecilia").append("nationalite", "AFR"));
                        break;
                    case "quit":
                        quit = true;
                        break;
                    default:
                        System.out.println("Question inconnue.");
                        break;
                }
            }

            mongoClient.close();
        }
    }

    private static void q5k(MongoCollection<Document> collection, int matchId, Document referee) {
        Document team = collection.find(Filters.elemMatch("matchs", Filters.eq("numeroMatch", matchId))).first();
        assert team != null;
        List<Document> matchs = (List<Document>) team.get("matchs");
        Document match = matchs.stream().filter(m -> m.getInteger("numeroMatch") == matchId).findFirst().orElse(null);
        assert match != null;
        String equipeRecevant = match.getString("equipeRecevant");
        String equipeRecue = match.getString("equipeReçue");
        String nationaliteArbitre = referee.getString("nationalite");

        if (!equipeRecevant.equals(nationaliteArbitre) && !equipeRecue.equals(nationaliteArbitre)) {
            match.put("arbitre", referee);
            Bson filter = Filters.eq("codeEquipe", team.getString("codeEquipe"));
            Bson update = Updates.set("matchs", matchs);
            collection.updateOne(filter, update);
            System.out.println("L'arbitre a été ajouté au match.");
        }
        else System.out.println("L'arbitre ne peut pas arbitrer ce match : il est de la même nationalité qu'une des équipes.");

    }



//    private static void q5k(MongoCollection<Document> collection, int matchId, Document referee) {
//        Document team = collection.find(Filters.elemMatch("matchs", Filters.eq("numeroMatch", matchId))).first();
//        assert team != null;
//        List<Document> matchs = (List<Document>) team.get("matchs");
//        Document match = matchs.stream().filter(m -> m.getInteger("numeroMatch") == matchId).findFirst().orElse(null);
//        assert match != null;
//        String equipeRecevant = match.getString("equipeRecevant");
//        String equipeRecue = match.getString("equipeReçue");
//        String nationaliteArbitre = referee.getString("nationalite");
//
//        if (!equipeRecevant.equals(nationaliteArbitre) && !equipeRecue.equals(nationaliteArbitre)) {
//            Bson filter = Filters.and(Filters.eq("matchs.numeroMatch", matchId), Filters.or(Filters.ne("equipeRecevant", nationaliteArbitre), Filters.ne("equipeReçue", nationaliteArbitre)));
//            Bson update = Updates.set("matchs.$.arbitre", referee);
//            collection.updateOne(filter, update);
//        }
//    }



//    private static void q5k(MongoCollection<Document> collection, int matchId, Document referee) {
//        Bson filter = Filters.eq("matchs.numeroMatch", matchId);
//        Bson update = Updates.set("matchs.$.arbitre", referee);
//        collection.updateOne(filter, update);
//    }


//    private static void q5k(MongoCollection<Document> collection, String matchId, Document referee) {
//        Document match = collection.find(Filters.eq("_id", new ObjectId(matchId))).first();
//        assert match != null;
//        if (!match.get("equipe1").equals(referee.get("nationalite")) && !match.get("equipe2").equals(referee.get("nationalite"))) {
//            collection.updateOne(Filters.eq("_id", new ObjectId(matchId)), Updates.set("**arbitre", referee));
//        }
//    }



    public static void q5j(MongoCollection<Document> collection) {
        List<Bson> pipelineEssais = Arrays.asList(
                Aggregates.unwind("$matchs"),
                Aggregates.unwind("$matchs.performances"),
                Aggregates.group("$matchs.performances.numeroJoueur",
                        Accumulators.first("nom", new Document("$arrayElemAt", Arrays.asList("$joueurs.nom", 0))),
                        Accumulators.first("prenom", new Document("$arrayElemAt", Arrays.asList("$joueurs.prenom", 0))),
                        Accumulators.sum("totalEssais", "$matchs.performances.essaisMarques")
                ),
                Aggregates.sort(Sorts.descending("totalEssais")),
                Aggregates.limit(1),
                includeFields("nom", "prenom", "totalEssais")
        );

        List<Bson> pipelinePoints = Arrays.asList(
                Aggregates.unwind("$matchs"),
                Aggregates.unwind("$matchs.performances"),
                Aggregates.group("$matchs.performances.numeroJoueur",
                        Accumulators.first("nom", new Document("$arrayElemAt", Arrays.asList("$joueurs.nom", 0))),
                        Accumulators.first("prenom", new Document("$arrayElemAt", Arrays.asList("$joueurs.prenom", 0))),
                        Accumulators.sum("totalPoints", "$matchs.performances.pointsMarques")
                ),
                Aggregates.sort(Sorts.descending("totalPoints")),
                Aggregates.limit(1),
                includeFields("nom", "prenom", "totalPoints")
        );

        sepQ5("j");
        System.out.println("Essais:");
        displayResult(collection, pipelineEssais);
        System.out.println("Points:");
        displayResult(collection, pipelinePoints);
        sep();
    }

//    //TODO show 2 diffrent player, 1 for each criteria
//    private static void q5j(MongoCollection<Document> collection) {
//        List<Bson> pipeline = Arrays.asList(
//                Aggregates.unwind("$joueurs"),
//                Aggregates.unwind("$matchs"),
//                Aggregates.unwind("$matchs.performances"),
//                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))),
//                Aggregates.group("$joueurs.numeroJoueur",
//                        Accumulators.first("nom", "$joueurs.nom"),
//                        Accumulators.first("prenom", "$joueurs.prenom"),
//                        Accumulators.sum("totalEssais", "$matchs.performances.essaisMarques"),
//                        Accumulators.sum("totalPoints", "$matchs.performances.pointsMarques")
//                ),
//                Aggregates.sort(Sorts.descending("totalEssais", "totalPoints")),
//                includeFields("nom", "prenom", "totalEssais", "totalPoints")
//        );
//
//        displayQuestionJson("j", collection, pipeline);
//    }


    private static void q5i(MongoCollection<Document> collection, String equipeE) {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.eq("codeEquipe", equipeE)),
                Aggregates.unwind("$matchs"),
                Aggregates.unwind("$matchs.performances"),
                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))),
                Aggregates.group("$joueurs.numeroJoueur",
                        Accumulators.first("nom", "$joueurs.nom"),
                        Accumulators.first("prenom", "$joueurs.prenom"),
                        Accumulators.sum("totalMatchs", 1)
                ),
                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$totalMatchs", new Document("$size", "$matchs"))))),
                includeFields("_id", "nom", "prenom", "totalMatchs")
        );

//        List<Bson> pipeline = Arrays.asList(
//                Aggregates.match(Filters.eq("codeEquipe", equipeE)),
//                Aggregates.unwind("$joueurs"),
//                Aggregates.unwind("$matchs"),
//                Aggregates.unwind("$matchs.performances"),
//                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))),
//                Aggregates.group("$joueurs.numeroJoueur",
//                        Accumulators.first("nom", "$joueurs.nom"),
//                        Accumulators.first("prenom", "$joueurs.prenom"),
//                        Accumulators.sum("totalMatchs", 1)
//                ),
//                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$totalMatchs", new Document("$cond", Arrays.asList(new Document("$isArray", "$matchs"), new Document("$size", "$matchs"), 0)) )))),
//                Aggregates.project(fields(
//                        include("_id", "nom", "prenom", "totalMatchs")
//                ))
//        );

//        List<Bson> pipeline = Arrays.asList(
//                Aggregates.match(Filters.eq("codeEquipe", equipeE)),
//                Aggregates.unwind("$joueurs"),
//                Aggregates.unwind("$matchs"),
//                Aggregates.unwind("$matchs.performances"),
//                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$matchs.performances.numeroJoueur", "$joueurs.numeroJoueur")))),
//                Aggregates.group("$joueurs.numeroJoueur",
//                        Accumulators.first("nom", "$joueurs.nom"),
//                        Accumulators.first("prenom", "$joueurs.prenom"),
//                        Accumulators.sum("totalMatchs", 1)
//                ),
//                Aggregates.match(Filters.expr(new Document("$eq", Arrays.asList("$totalMatchs", new Document("$size", "$matchs"))))),
//                Aggregates.project(fields(
//                        include("_id", "nom", "prenom", "totalMatchs")
//                ))
//        );

        displayQuestionJson("i", collection, pipeline);
    }



    private static void q5h(MongoCollection<Document> collection, String equipeE) {
        // Étape 1: Obtenir une liste de tous les numéros de joueurs qui ont joué dans les matchs
        List<Integer> playerNumbersWhoPlayed = collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("codeEquipe", equipeE)),
                Aggregates.unwind("$matchs"),
                Aggregates.unwind("$matchs.performances"),
                Aggregates.group("$matchs.performances.numeroJoueur")
        )).map(document -> document.getInteger("_id")).into(new ArrayList<>());

//        playerNumbersWhoPlayed.remove(0);
//        System.out.println(playerNumbersWhoPlayed);

        // Étape 2: Obtenir les joueurs qui ne sont pas dans la liste des joueurs qui ont joué
        Bson filter = Filters.and(
                Filters.eq("codeEquipe", equipeE),
                Filters.not(Filters.in("joueurs.numeroJoueur", playerNumbersWhoPlayed))
        );
        FindIterable<Document> documents = collection.find(filter);

        sepQ5("h");
        for (Document document : documents) {
            List<Document> joueurs = (List<Document>) document.get("joueurs");
            for (Document joueur : joueurs) {
                System.out.println("Nom: " + joueur.getString("nom") + ", Prenom: " + joueur.getString("prenom"));
            }
        }
        sep();
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
                includeFields("nom", "prenom")

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
                includeFields("nom", "prenom", "totalMatchs", "totalEssais", "totalPoints") // Inclure uniquement les champs nécessaires

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
                includeFields("joueurs.nom", "joueurs.prenom") // Inclure uniquement le nom et le prénom du joueur
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
            includeFields("joueurs.nom", "joueurs.prenom") // Inclure uniquement le nom et le prénom du joueur
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
                includeFields("matchs.arbitre.nom", "matchs.arbitre.prenom", "matchs.arbitre.nationalite")
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
                includeFields("matchs.numeroMatch", "matchs.date", "matchs.evenement",
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

    private static Bson includeFields(String ...fieldsNames) {
        return Aggregates.project(
                fields(
                        excludeId(),
                        include(fieldsNames)
                )
        );
    }

    private static void displayQuestionJson(String q, MongoCollection<Document> collection, List<Bson> pipeline) {
        sepQ5(q);
        displayResult(collection, pipeline);
        sep();
    }

    private static void displayResult(MongoCollection<Document> collection, List<Bson> pipeline) {
        for (Document doc : collection.aggregate(pipeline)) {
            System.out.println(doc.toJson(JsonWriterSettings.builder().indent(true).build()));
        }
    }

    static void sepQ5(String q) {
        System.out.println("--- Q5." + q + " ---");
    }

    static void sep() {
        System.out.println("---");
    }
}
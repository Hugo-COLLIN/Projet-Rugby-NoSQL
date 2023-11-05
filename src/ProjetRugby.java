/**
 * Hugo COLLIN - 05/11/2023
 */

import com.mongodb.client.model.*;
import org.bson.*;
import org.bson.conversions.Bson;
import com.mongodb.client.*;

import java.util.*;
import java.util.function.Predicate;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

/**
 * Classe du Projet Rugby.
 */
public class ProjetRugby {

    /**
     * Prédicat pour vérifier que le code de l'équipe est de 3 lettres.
     */
    public static final Predicate<String> CODE_EQUIPE_PREDICATE = s -> s.length() == 3;

    /**
     * Message d'erreur pour le code de l'équipe.
     */
    public static final String CODE_EQUIPE_ERR_MSG = "Veuillez entrer un code de 3 lettres.";

    /**
     * Prédicat pour vérifier que la longueur de la chaîne est inférieure ou égale à 20.
     */
    public static final Predicate<String> STRING_LENGTH_PREDICATE = s -> s.length() <= 20;

    /**
     * Exécute les requêtes de la question 5.
     * @param args Arguments:
     *             - args[0]: URI de la base de données
     *             - args[1]: Nom de la base de données
     *             - args[2]: Nom de la collection
     */
    public static void main(String[] args) {
        String uri = args.length > 0 ? args[0] : "mongodb://localhost:27017";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase(args.length > 1 ? args[1] : "ProjetRugby");
            MongoCollection<Document> collection = sampleTrainingDB.getCollection(args.length > 2 ? args[2] : "equipes");
            Scanner scanner = new Scanner(System.in);
            String question;
            String equipeE1, equipeE2, equipeE3, dateD, arbitreANom, arbitreAPrenom, arbitreANationalite;
            int pointsP, matchId;
            boolean quit = false;

            while (!quit) {
                System.out.print("Choisissez une question (a, b, c, d, e, f, g, h, i, j, k) ou écrivez quit pour quitter: ");
                question = scanner.nextLine();

                switch (question) {
                    case "a":
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5a(collection, equipeE1);
                        break;
                    case "b":
                        dateD = UserInput.questionString(scanner, "Date du match (testé avec \"2023-09-22\"): ");
                        pointsP = UserInput.questionInt(scanner, "Nombre de points (testé avec 10): ", i -> i > 0, "Veuillez entrer un nombre positif.");
                        q5b(collection, dateD, pointsP);
                        break;
                    case "c":
                        arbitreANom = UserInput.questionString(scanner, "Nom de l'arbitre (testé avec \"Barnes\"): ", STRING_LENGTH_PREDICATE);
                        q5c(collection, arbitreANom);
                        break;
                    case "d":
                        dateD = UserInput.questionString(scanner, "Date du match (testé avec \"2023-09-25\"): ");
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        equipeE2 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ESP\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5d(collection, dateD, equipeE1, equipeE2);
                        break;
                    case "e":
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5e(collection, equipeE1);
                        break;
                    case "f":
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5f(collection, equipeE1);
                        break;
                    case "g":
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe 1 (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        equipeE2 = UserInput.questionString(scanner, "Code de l'équipe 2 (testé avec \"ESP\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        equipeE3 = UserInput.questionString(scanner, "Code de l'équipe 3 (testé avec \"FRA\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5g(collection, equipeE1, equipeE2, equipeE3);
                        break;
                    case "h":
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5h(collection, equipeE1);
                        break;
                    case "i":
                        equipeE1 = UserInput.questionString(scanner, "Code de l'équipe (testé avec \"ENG\"): ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5i(collection, equipeE1);
                        break;
                    case "j":
                        q5j(collection);
                        break;
                    case "k":
                        matchId = UserInput.questionInt(scanner, "Numéro du match: ", i -> i > 0, "Veuillez entrer un nombre positif.");
                        arbitreANom = UserInput.questionString(scanner, "Nom de l'arbitre: ", STRING_LENGTH_PREDICATE);
                        arbitreAPrenom = UserInput.questionString(scanner, "Prénom de l'arbitre: ", STRING_LENGTH_PREDICATE);
                        arbitreANationalite = UserInput.questionString(scanner, "Nationalité de l'arbitre: ", CODE_EQUIPE_PREDICATE, CODE_EQUIPE_ERR_MSG);
                        q5k(collection, matchId, new Document("nom", arbitreANom).append("prenom", arbitreAPrenom).append("nationalite", arbitreANationalite.toUpperCase()));
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

    /**
     * Affiche pour tous les joueurs de l'équipe E : leur temps de jeu, le nombre
     * d'essais marqués, le nombre de points marqués, et le coefficient (nombre de
     * points/durée de jeu), par ordre décroissant du coefficient.
     * @param collection Collection des équipes
     * @param equipeE Équipe E
     */
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


        DisplayData.displayQuestionJson("a", collection, pipeline);
    }

    /**
     * Affiche les matchs qui ont eu lieu à la dateD et qui ont rapporté plus de points que le nombre pointsP.
     * @param collection Collection des équipes
     * @param dateD Date du match
     * @param pointsP Nombre de points
     */
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

        DisplayData.displayQuestionJson("b", collection, pipeline);
    }

    /**
     * Affiche les équipes qui recevaient et qui ont été arbitrés par un arbitre A.
     * @param collection Collection des équipes
     * @param arbitreA Arbitre A
     */
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

        DisplayData.displayQuestionJson("c", collection, pipeline);
    }

    /**
     * Affiche tous les joueurs de l'équipe E1 qui ont débuté un match à la dateD contre l'équipe E2.
     * @param collection Collection des équipes
     * @param dateD Date du match
     * @param equipeE1 Équipe E1
     * @param equipeE2 Équipe E2
     */
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

        DisplayData.displayQuestionJson("d", collection, pipeline);
    }

    /**
     * Affiche le nom et le prénom des joueurs de l'équipe E qui sont entrés en cours de jeu.
     * @param collection Collection des équipes
     * @param equipeE Équipe E
     */
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

        DisplayData.displayQuestionJson("e", collection, pipeline);
    }

    /**
     * Affiche pour caque joueur de l'équipe E le nombre total de matchs joués, le nombre total d'essais marqués et le nombre total de points marqués.
     * @param collection Collection des équipes
     * @param equipeE Équipe E
     */
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

        DisplayData.displayQuestionJson("f", collection, pipeline);
    }

    /**
     * Affiche les joueurs de l'équipe E1 qui ont joué contre les équipes E2 et E3.
     * @param collection Collection des équipes
     * @param equipeE1 Équipe E1
     * @param equipeE2 Équipe E2
     * @param equipeE3 Équipe E3
     */
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

        DisplayData.displayQuestionJson("g", collection, pipeline);
    }

    /**
     * Affiche les joueurs qui n'ont joué aucun match de leur équipe.
     * @param collection Collection des équipes
     * @param equipeE Équipe
     */
    private static void q5h(MongoCollection<Document> collection, String equipeE) {
        // Étape 1: Obtenir une liste de tous les numéros de joueurs qui ont joué dans les matchs
        List<Integer> playerNumbersWhoPlayed = collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("codeEquipe", equipeE)),
                Aggregates.unwind("$matchs"),
                Aggregates.unwind("$matchs.performances"),
                Aggregates.group("$matchs.performances.numeroJoueur")
        )).map(document -> document.getInteger("_id")).into(new ArrayList<>());

        // Étape 2: Obtenir les joueurs qui ne sont pas dans la liste des joueurs qui ont joué
        Bson filter = Filters.and(
                Filters.eq("codeEquipe", equipeE),
                Filters.not(Filters.in("joueurs.numeroJoueur", playerNumbersWhoPlayed))
        );
        FindIterable<Document> documents = collection.find(filter);

        DisplayData.sepQ5("h");
        for (Document document : documents) {
            List<Document> joueurs = (List<Document>) document.get("joueurs");
            for (Document joueur : joueurs) {
                System.out.println("Nom: " + joueur.getString("nom") + ", Prenom: " + joueur.getString("prenom"));
            }
        }
        DisplayData.sep();
    }

    /**
     * Affiche les joueurs qui ont joué tous les matchs de leur équipe.
     * @param collection Collection des équipes
     * @param equipeE Équipe
     */
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

        DisplayData.displayQuestionJson("i", collection, pipeline);
    }

    /**
     * Affiche le joueur qui a marqué le plus d'essais et celui qui a marqué le plus de points.
     * @param collection Collection des équipes
     */
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

        DisplayData.sepQ5("j");
        System.out.println("Essais:");
        DisplayData.displayResult(collection, pipelineEssais);
        System.out.println("Points:");
        DisplayData.displayResult(collection, pipelinePoints);
        DisplayData.sep();
    }

    /**
     * Ajoute un arbitre à un match.
     * @param collection Collection des équipes
     * @param matchId Identifiant du match
     * @param referee Arbitre à ajouter
     */
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

    /**
     * Inclure uniquement les champs spécifiés.
     * @param fieldsNames Noms des champs à inclure
     * @return Bson correspondant à la projection
     */
    private static Bson includeFields(String ...fieldsNames) {
        return Aggregates.project(
                fields(
                        excludeId(),
                        include(fieldsNames)
                )
        );
    }
}
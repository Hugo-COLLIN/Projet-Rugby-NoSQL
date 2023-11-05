/**
 * Hugo COLLIN - 05/11/2023
 */

import java.util.Scanner;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Classe utilitaire pour la saisie de l'utilisateur.
 */
public class UserInput {

    /**
     * Demande à l'utilisateur d'entrer une valeur.
     *
     * @param scanner  Scanner
     * @param question Question
     * @return Valeur entrée par l'utilisateur
     */
    static String questionString(Scanner scanner, String question) {
        return questionString(scanner, question, s -> true);
    }

    /**
     * Demande à l'utilisateur d'entrer une valeur entière.
     *
     * @param scanner Scanner
     * @param question Question
     * @return Valeur entière entrée par l'utilisateur
     */
    static int questionInt(Scanner scanner, String question) {
        return questionInt(scanner, question, s -> true);
    }


    /**
     * Demande à l'utilisateur d'entrer une valeur.
     * La valeur doit respecter les conditions.
     * Un message d'erreur est affiché si la valeur ne respecte pas les conditions.
     *
     * @param scanner    Scanner
     * @param question   Question
     * @param conditions Conditions de la valeur
     * @return Valeur entrée par l'utilisateur
     */
    static String questionString(Scanner scanner, String question, Predicate<String> conditions) {
        return questionString(scanner, question, conditions, "Veuillez entrer une valeur valide.");
    }

    /**
     * Demande à l'utilisateur d'entrer une valeur entière.
     * La valeur doit respecter les conditions.
     * Un message d'erreur est affiché si la valeur ne respecte pas les conditions.
     *
     * @param scanner Scanner
     * @param question Question
     * @param conditions Conditions de la valeur
     * @return Valeur entière entrée par l'utilisateur
     */
    static int questionInt(Scanner scanner, String question, Predicate<Integer> conditions) {
        return questionInt(scanner, question, conditions, "Veuillez entrer une valeur valide.");
    }

    /**
     * Demande à l'utilisateur d'entrer une valeur.
     * La valeur doit respecter les conditions.
     * Un message d'erreur est affiché si la valeur ne respecte pas les conditions.
     *
     * @param scanner Scanner
     * @param question Question
     * @param conditions Conditions de la valeur
     * @param conditionErrorMessage Message d'erreur si la valeur ne respecte pas les conditions
     * @return Valeur entrée par l'utilisateur
     */
    static String questionString(Scanner scanner, String question, Predicate<String> conditions, String conditionErrorMessage) {
        return question(scanner, question, Function.identity(), conditions, conditionErrorMessage);
    }

    /**
     * Demande à l'utilisateur d'entrer une valeur entière.
     * La valeur doit respecter les conditions.
     * Un message d'erreur spécifique est affiché si la valeur ne respecte pas les conditions.
     *
     * @param scanner Scanner
     * @param question Question
     * @param conditions Conditions de la valeur
     * @param conditionErrorMessage Message d'erreur si la valeur ne respecte pas les conditions
     * @return Valeur entière entrée par l'utilisateur
     */
    static int questionInt(Scanner scanner, String question, Predicate<Integer> conditions, String conditionErrorMessage) {
        return question(scanner, question, Integer::parseInt, conditions, conditionErrorMessage);
    }


    /**
     * Demande à l'utilisateur d'entrer une valeur.
     * La valeur doit respecter les conditions.
     * Un message d'erreur spécifique est affiché si la valeur ne respecte pas les conditions.
     *
     * @param scanner               Scanner
     * @param question              Question
     * @param converter             Convertisseur de la valeur
     * @param conditions            Conditions de la valeur
     * @param conditionErrorMessage Message d'erreur si la valeur ne respecte pas les conditions
     * @return Valeur entrée par l'utilisateur
     */
    static <T> T question(Scanner scanner, String question, Function<String, T> converter, Predicate<T> conditions, String conditionErrorMessage) {
        System.out.print(question);
        String input = scanner.nextLine();
        if (input.isEmpty()) {
            System.out.println("Veuillez entrer une valeur.");
            return question(scanner, question, converter, conditions, conditionErrorMessage);
        }
        T value;
        try {
            value = converter.apply(input);
        } catch (Exception e) {
            System.out.println(conditionErrorMessage);
            return question(scanner, question, converter, conditions, conditionErrorMessage);
        }
        if (!conditions.test(value)) {
            System.out.println(conditionErrorMessage);
            return question(scanner, question, converter, conditions, conditionErrorMessage);
        }

        return value;
    }
}
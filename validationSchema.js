// Connectez-vous à MongoDB
mongo

// Créez la base de données ProjetRugby
use ProjetRugby

// Créez la collection equipes avec la validation de schéma
db.createCollection("equipes", {
   validator: {
      $jsonSchema: {
         bsonType: "object",
         required: [ "codeEquipe", "pays", "couleurMaillot", "nomEntraineur", "joueurs", "matchs" ],
         properties: {
            codeEquipe: {
               bsonType: "string",
               description: "must be a string and is required"
            },
            pays: {
               bsonType: "object",
               required: [ "codePays", "nomPays" ],
               properties: {
                  codePays: {
                     bsonType: "string",
                     description: "must be a string and is required"
                  },
                  nomPays: {
                     bsonType: "string",
                     description: "must be a string and is required"
                  }
               }
            },
            couleurMaillot: {
               bsonType: "string",
               description: "must be a string and is required"
            },
            nomEntraineur: {
               bsonType: "string",
               description: "must be a string and is required"
            },
            joueurs: {
               bsonType: "array",
               items: {
                  bsonType: "object",
                  required: [ "numeroJoueur", "nom", "prenom", "poste" ],
                  properties: {
                     numeroJoueur: {
                        bsonType: "int",
                        description: "must be an integer and is required"
                     },
                     nom: {
                        bsonType: "string",
                        description: "must be a string and is required"
                     },
                     prenom: {
                        bsonType: "string",
                        description: "must be a string and is required"
                     },
                     poste: {
                        bsonType: "object",
                        required: [ "numeroPoste", "libelle" ],
                        properties: {
                           numeroPoste: {
                              bsonType: "int",
                              description: "must be an integer and is required"
                           },
                           libelle: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           }
                        }
                     }
                  }
               }
            },
            matchs: {
               bsonType: "array",
               items: {
                  bsonType: "object",
                  required: [ "numeroMatch", "date", "evenement", "stade", "equipeRecevant", "equipeReçue", "nombrePoints", "nombreEssais", "arbitre", "nombreSpectateurs", "performances" ],
                  properties: {
                     numeroMatch: {
                        bsonType: "int",
                        description: "must be an integer and is required"
                     },
                     date: {
                        bsonType: "date",
                        description: "must be a date and is required"
                     },
                     evenement: {
                        bsonType: "object",
                        required: [ "numeroEvenement", "nomEvenement", "dateEvenement", "lieuEvenement" ],
                        properties: {
                           numeroEvenement: {
                              bsonType: "int",
                              description: "must be an integer and is required"
                           },
                           nomEvenement: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           },
                           dateEvenement: {
                              bsonType: "date",
                              description: "must be a date and is required"
                           },
                           lieuEvenement: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           }
                        }
                     },
                     stade: {
                        bsonType: "object",
                        required: [ "numeroStade", "nom", "ville", "capacite" ],
                        properties: {
                           numeroStade: {
                              bsonType: "int",
                              description: "must be an integer and is required"
                           },
                           nom: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           },
                           ville: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           },
                           capacite: {
                              bsonType: "int",
                              description: "must be an integer and is required"
                           }
                        }
                     },
                     equipeRecevant: {
                        bsonType: "string",
                        description: "must be a string and is required"
                     },
                     equipeReçue: {
                        bsonType: "string",
                        description: "must be a string and is required"
                     },
                     nombrePoints: {
                        bsonType: "int",
                        description: "must be an integer and is required"
                     },
                     nombreEssais: {
                        bsonType: "int",
                        description: "must be an integer and is required"
                     },
                     arbitre: {
                        bsonType: "object",
                        required: [ "numeroArbitre", "nom", "prenom", "nationalite" ],
                        properties: {
                           numeroArbitre: {
                              bsonType: "int",
                              description: "must be an integer and is required"
                           },
                           nom: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           },
                           prenom: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           },
                           nationalite: {
                              bsonType: "string",
                              description: "must be a string and is required"
                           }
                        }
                     },
                     nombreSpectateurs: {
                        bsonType: "int",
                        description: "must be an integer and is required"
                     },
                     performances: {
                        bsonType: "array",
                        items: {
                           bsonType: "object",
                           required: [ "numeroJoueur", "tempsDeJeu", "essaisMarques", "pointsMarques", "debutMatch" ],
                           properties: {
                              numeroJoueur: {
                                 bsonType: "int",
                                 description: "must be an integer and is required"
                              },
                              tempsDeJeu: {
                                 bsonType: "int",
                                 description: "must be an integer and is required"
                              },
                              essaisMarques: {
                                 bsonType: "int",
                                 description: "must be an integer and is required"
                              },
                              pointsMarques: {
                                 bsonType: "int",
                                 description: "must be an integer and is required"
                              },
                              debutMatch: {
                                 bsonType: "bool",
                                 description: "must be a boolean and is required"
                              }
                           }
                        }
                     }
                  }
               }
            }
         }
      }
   }
})

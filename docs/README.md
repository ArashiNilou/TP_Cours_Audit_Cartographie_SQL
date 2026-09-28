# Documentation du projet

Point d'entrée de toute la documentation. Le [README principal](../README.md)
présente le projet et son installation ; les documents ci-dessous détaillent
chaque aspect.

## Par TP

| TP | Sujet | Documents principaux |
|---|---|---|
| TP1 | Modélisation 3NF | [Dictionnaire de données](data-dictionary.md), [MCD](data-model/mcd.mmd), [MLD](data-model/mld.mmd), [DBML](data-model/model.dbml), [schéma SQL](../database/schema/01_schema.sql) |
| TP2 | Pipeline temps quasi réel | [Schéma du pipeline](architecture/pipeline.mmd), [Architecture : Data Lake, contrats d'événements, choix](architecture/README.md) |
| TP3 | Audit qualité et nettoyage | [Audit qualité : matrice, règles, résultats, synthèse orale](quality/README.md), [schéma qualité](architecture/data-quality.mmd) |

## Par thème

| Thème | Document | Contenu |
|---|---|---|
| Cartographie | [Source → cible](cartography/source-to-target-mapping.md) | Lignage champ par champ, du JSON France Travail / API Géo aux colonnes PostgreSQL, règles de rejet |
| | [Schéma du pipeline](architecture/pipeline.mmd) | Composants, formats, fréquences et supervision |
| | [Schéma qualité](architecture/data-quality.mmd) | Place de l'audit dans le cheminement des données |
| Modèle de données | [Dictionnaire](data-dictionary.md) | Toutes les colonnes, vues et index |
| | [DBML](data-model/model.dbml) | Schéma physique complet (11 tables) à coller dans [dbdiagram.io](https://dbdiagram.io) |
| | [MCD](data-model/mcd.mmd) / [MLD](data-model/mld.mmd) | Modèles conceptuel et logique (Mermaid) |
| | [Schéma illustré](data-model/schema-overview.png) | Vue d'ensemble des tables métier |
| Architecture | [Architecture de la plateforme](architecture/README.md) | Data Lake (zones, formats, volumétrie, rétention), contrats d'événements Kafka (`tp2.offer.v1`, `tp2.aggregated_offer.v1`), choix d'architecture justifiés |
| Qualité | [Audit qualité et nettoyage](quality/README.md) | Exécution, matrice des 17 contrôles sur 5 dimensions, règles de correction R01 à R04, mesures avant / après, synthèse orale |
| Exploitation | [Guide d'exploitation](operations/runbook.md) | Vérifications, incidents, sauvegarde, rejeu, purge |
| Gouvernance | [Licences et RGPD](governance/licences-rgpd.md) | Conditions des sources, données personnelles, secrets |
| Limites | [Limites connues](known-limitations.md) | Limites mesurées et pistes d'amélioration |
| Restitution | [Présentation](presentation/project-presentation.pptx) | Diaporama global des 3 TP |

## Lire les schémas Mermaid

Le MCD et le MLD métier sont repris dans le README (affichés directement par
GitHub). Pour ouvrir un fichier `.mmd` seul, utiliser l'extension Mermaid de
l'éditeur ou <https://mermaid.live>.

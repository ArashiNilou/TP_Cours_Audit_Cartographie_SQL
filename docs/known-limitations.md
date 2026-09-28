# Limites connues

Ces limites sont assumées et mesurées. Elles sont classées par nature, avec leur
impact et la piste d'amélioration.

## Données sources

| Limite | Mesure | Impact | Traitement retenu |
|---|---|---|---|
| Offres sans salaire exploitable | ≈ 27 % | Statistiques de salaire calculées sur les ≈ 73 % restants | Laissé à NULL, jamais imputé (TP3) |
| Offres sans compétence listée | ≈ 12 % | Absentes des classements de compétences | Conservées, mesurées (contrôle qualité) |
| Commune non retrouvée dans l'API Géo | ≈ 1,5 % | Nom et position fournis par France Travail, non vérifiés | Conservées avec le statut `not_found` |
| Offres sans lien de candidature direct | ≈ 73 % | Pas de bouton « Postuler » dans la fiche | Lien vers l'offre France Travail toujours proposé |
| Plafond de 1 149 résultats par recherche API | — | Une recherche nationale unique est incomplète | Collecte possible département par département (`FT_PRODUCER_ALL_DEPARTEMENTS`) |

## Transformations

| Limite | Détail | Piste d'amélioration |
|---|---|---|
| Salaire estimé | Moyenne de la fourchette ; horaire annualisé sur 35 h × 52 semaines, journalier sur 218 jours : ne tient pas compte du temps partiel | Utiliser `dureeTravailLibelle` pour proratiser |
| Type de compétence | Toutes les compétences sont classées `Savoir-faire` ; les savoir-être (`qualitesProfessionnelles`) ne sont pas chargés en base (visibles seulement dans la fiche) | Charger `qualitesProfessionnelles` avec `type_competence = 'Savoir-être'` |
| Domaine professionnel | Déduit de la première lettre du code ROME (14 grands domaines), pas du référentiel ROME complet | Charger le référentiel ROME 4.0 officiel |
| Offres expirées | Une offre retirée par France Travail reste en base | Marquer ou purger les offres absentes des dernières collectes |

## Plateforme

| Limite | Détail | Piste d'amélioration |
|---|---|---|
| Kafka mono-broker | Réplication 1 : pas de tolérance à la panne du broker | 3 brokers, réplication 3 |
| Dash mono-processus | 1 processus, 8 threads, cache en mémoire | Cache partagé (Redis) pour passer à plusieurs processus |
| Mémoire du producteur | La liste des offres déjà publiées est perdue au redémarrage : elles sont republiées une fois | Sans conséquence sur les données (dédoublonnage par `event_id` et par Spark) ; persister l'état si le volume grandit |
| Rétention du Data Lake | Aucune purge automatique ; `curated` grossit d'un dossier toutes les 30 min (726 Mo au 28/09/2026) | Purge planifiée (commande dans le [guide d'exploitation](operations/runbook.md)) |
| Alertes | Supervision visible dans Grafana, mais aucune alerte envoyée | Règles d'alerte Grafana / Alertmanager |
| Secrets | Mots de passe par défaut en local | Gestionnaire de secrets en production |

## Anomalies corrigées

| Anomalie | Correction | Référence |
|---|---|---|
| Salaires irréalistes (ex. 12 €/an) | Parseur corrigé + 7 614 valeurs neutralisées | Règle `R03_SALAIRE` |
| Doublons de compétences et d'entreprises | Fusion + index uniques | Règles `R01`, `R02` |
| Domaine ROME « A » libellé « Arts et façonnage » au lieu de « Agriculture » | Table de correspondance corrigée + 52 métiers (611 offres) mis à jour | Règle `R04_DOMAINE_ROME` |

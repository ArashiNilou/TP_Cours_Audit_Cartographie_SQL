# Licences, conditions d'utilisation et données personnelles

## Sources et conditions de réutilisation

| Source | Accès | Conditions | Obligations respectées dans le projet |
|---|---|---|---|
| API « Offres d'emploi v2 » (France Travail) | Compte développeur sur [francetravail.io](https://francetravail.io), identifiants OAuth2 (`FT_CLIENT_ID`, `FT_CLIENT_SECRET`) | Conditions générales d'utilisation de francetravail.io, à relire avant toute diffusion publique | Identifiants hors du dépôt (`.env` ignoré par Git) ; appels limités (pagination avec pause entre les pages, collecte toutes les 15 min) ; source citée ; lien vers l'offre d'origine dans la fiche détaillée |
| API Géo — communes (État) | Libre, sans clé | Données publiques sous Licence Ouverte Etalab 2.0 | Source citée ; données non altérées dans la zone brute |

Le projet est un travail pédagogique. Il ne republie pas les offres : il les
analyse et renvoie vers France Travail pour la consultation et la candidature.

## Données personnelles (RGPD)

Une offre peut contenir des informations sur la personne à contacter
(`contact.nom`, `contact.courriel`, `contact.telephone`, `contact.coordonnees*`).

| Mesure | Détail |
|---|---|
| Minimisation | Aucun champ de contact personnel n'est chargé dans PostgreSQL ni affiché dans Dash ou Metabase. Seul le lien de candidature public (`contact.urlPostulation`) est proposé. |
| Stockage brut | La zone `raw` du Data Lake conserve l'offre telle que publiée, donc potentiellement ces champs. Elle n'est accessible qu'à l'intérieur de Docker et est montée en lecture seule dans Dash. |
| Finalité | Analyse statistique du marché de l'emploi, sans profilage de personnes. |
| Entreprises | Le nom d'entreprise est une donnée publique de l'offre ; les offres anonymes sont rattachées à une entreprise « anonyme » unique, sans tentative de réidentification. |
| Durée de conservation | Non automatisée (voir [limites connues](../known-limitations.md)). Pour un usage réel, prévoir la purge des offres expirées et de la zone brute au-delà d'une durée définie. |

## Secrets et sécurité

| Élément | Règle |
|---|---|
| Identifiants France Travail, mots de passe | Uniquement dans `.env` (non versionné) ; `.env.example` ne contient aucune valeur secrète |
| Mots de passe par défaut (`postgres`, Grafana `admin`) | Acceptables uniquement en local ; à remplacer dans `.env` avant toute exposition réseau |
| Grafana | Publié sur `127.0.0.1` uniquement |
| Data Lake dans Dash | Montage en lecture seule (`:ro`) |

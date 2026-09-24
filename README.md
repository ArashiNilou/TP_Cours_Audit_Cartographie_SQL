# TP - Audit, cartographie et modélisation de données : tensions sur les compétences numériques

## 1. Sujet et problématique métier

### Domaine

Emploi, marché du travail et recrutement.

### Contexte

Les acteurs publics et privés du marché du travail (opérateurs de l'emploi,
collectivités territoriales, cabinets de recrutement) cherchent à comprendre,
en temps quasi réel, quelles compétences numériques sont les plus recherchées
et sur quels territoires. Les offres d'emploi diffusées par les plateformes
nationales constituent une matière première précieuse, mais elles arrivent
sous forme de flux JSON semi-structurés, imbriqués et hétérogènes, tandis que
les référentiels nationaux de métiers et de géographie sont publiés dans des
formats et des granularités différents.

### Problématique

Comment transformer des flux d'offres d'emploi semi-structurés issus d'une
API et des référentiels nationaux hétérogènes en un entrepôt relationnel en
troisième forme normale, fiable et interrogeable, permettant d'identifier en
temps réel les compétences les plus recherchées par type de contrat et par
bassin d'emploi ?

### Objectif

* Concevoir un dictionnaire de données couvrant l'ensemble des champs
  mobilisés pour la modélisation.
* Formaliser le Modèle Conceptuel des Données et le Modèle Logique des
  Données correspondants.
* Générer le code DBML complet, prêt à être importé dans dbdiagram.io.
* Livrer un script DDL PostgreSQL complet, avec contraintes, index, données
  de test et requêtes d'analyse orientées tableau de bord RH.

## 2. Sources de données réelles

| Nom | Organisation | URL | Format | Nature | Description métier |
|---|---|---|---|---|---|
| API « Offres d'emploi v2 » | France Travail | <https://francetravail.io/produits-partages/catalogue/offres-emploi> | JSON | Semi-structurée, objets imbriqués | Flux d'offres d'emploi avec objets imbriqués `lieuTravail`, `entreprise`, tableau `competences[]` et bloc `salaire` en texte libre. Constitue la table de faits du modèle. |
| Référentiel ROME 4.0 | France Travail (Open Data data.gouv.fr) | <https://www.data.gouv.fr/fr/datasets/repertoire-operationnel-des-metiers-et-des-emplois-rome/> | CSV | Structurée | Arborescence des domaines professionnels, fiches métiers et codes ROME associés. Sert de référentiel stable pour classer chaque offre par métier. |
| Base Adresse Nationale (BAN) / référentiel des communes INSEE | Institut national de l'information géographique (IGN) et INSEE (Open Data data.gouv.fr) | <https://www.data.gouv.fr/fr/datasets/base-adresse-nationale/> | CSV | Structurée | Codes INSEE, codes postaux, libellés de communes et coordonnées GPS, permettant de rattacher chaque offre à un bassin d'emploi identifié de manière stable. |

### Points d'audit spécifiques à chaque source

* **API Offres d'emploi** : les objets `entreprise`, `salaire` et
  `lieuTravail` sont parfois absents ou partiellement renseignés ; le champ
  salaire est un texte libre nécessitant un parsing par expression régulière
  pour en extraire une valeur numérique exploitable ; le tableau
  `competences[]` peut être vide, contenir des doublons ou mélanger
  savoir-faire et savoir-être sans distinction explicite.
* **Référentiel ROME 4.0** : certains codes ROME publiés dans les offres
  peuvent être absents du millésime du référentiel téléchargé ; un contrôle
  de rapprochement est nécessaire avant tout chargement.
* **BAN / référentiel INSEE** : les codes postaux ne sont pas des
  identifiants stables de commune (plusieurs codes postaux par commune ou
  inversement) ; seul le code INSEE est retenu comme clé de rattachement
  géographique fiable dans le modèle.

## 3. Dictionnaire de données

| Champ | Description fonctionnelle | Type SQL cible | Exemple de valeur | Contraintes & règle métier |
|---|---|---|---|---|
| `offre_id` | Identifiant technique interne de l'offre | `bigint` | `1` | PK générée automatiquement |
| `source_offre_id` | Identifiant de l'offre côté API France Travail | `varchar(20)` | `178XYZW` | NOT NULL, UNIQUE ; garantit la traçabilité avec la source |
| `libelle_poste` | Intitulé du poste proposé | `varchar(200)` | `Data Engineer / DBA PostgreSQL (F/H)` | NOT NULL |
| `description` | Description synthétique de l'offre | `text` | `Au sein du pôle Data, vous concevez...` | Nullable, texte libre |
| `date_publication` | Date de publication de l'offre | `date` | `2026-09-18` | NOT NULL |
| `type_contrat` | Type de contrat proposé | `varchar(5)` | `CDI` | NOT NULL, CHECK IN ('CDI', 'CDD', 'MIS', 'SAI', 'CCE') |
| `duree_travail` | Régime horaire déclaré | `varchar(100)` | `35H Horaires normaux` | Nullable |
| `salaire_brut_annuel_estime` | Salaire brut annuel estimé, extrait du texte libre par parsing | `numeric(10,2)` | `41500.00` | Nullable, CHECK >= 0 |
| `entreprise_id` | Référence à l'entreprise recruteuse | `bigint` | `1` | FK vers `entreprise`, NOT NULL |
| `raison_sociale` | Nom de l'entreprise recruteuse | `varchar(150)` | `TECH INNOVATION` | Nullable si `entreprise_anonyme = true` ; NOT NULL sinon (règle métier appliquée par `CHECK`) |
| `entreprise_anonyme` | Indique si l'entreprise a choisi de rester anonyme dans l'offre | `boolean` | `false` | NOT NULL, défaut `false` |
| `rome_code` | Code du métier ROME associé à l'offre | `varchar(5)` | `M1805` | FK vers `metier_rome`, NOT NULL |
| `libelle_fiche_metier` | Libellé de la fiche métier ROME | `varchar(150)` | `Études et développement informatique` | NOT NULL |
| `domaine_professionnel` | Domaine professionnel de rattachement du métier | `varchar(150)` | `Systèmes d'information et télécommunications` | NOT NULL |
| `libelle_competence` | Libellé de la compétence technique ou comportementale | `varchar(150)` | `Langage SQL` | NOT NULL, UNIQUE |
| `type_competence` | Nature de la compétence | `varchar(20)` | `Savoir-faire` | NOT NULL, CHECK IN ('Savoir-faire', 'Savoir-être') |
| `statut_exigence` | Caractère exigé ou souhaité d'une compétence pour une offre donnée | `varchar(1)` | `E` | NOT NULL, CHECK IN ('E', 'S') |
| `code_insee` | Code officiel INSEE de la commune | `char(5)` | `44172` | PK de `commune`, FK dans `offre`, NOT NULL |
| `code_postal` | Code postal associé à la commune | `varchar(5)` | `44980` | NOT NULL |
| `nom_commune` | Libellé officiel de la commune | `varchar(100)` | `Sainte-Luce-sur-Loire` | NOT NULL |
| `latitude` | Latitude du centroïde de la commune | `numeric(9,6)` | `47.249400` | Nullable, CHECK entre -90 et 90 |
| `longitude` | Longitude du centroïde de la commune | `numeric(9,6)` | `-1.486200` | Nullable, CHECK entre -180 et 180 |

## 4. Identification des entités, cardinalités et justification

### Entités retenues

* **COMMUNE** : bassin d'emploi identifié par le code INSEE.
* **ENTREPRISE** : entité recruteuse, pouvant être anonyme dans l'offre
  publiée.
* **METIER_ROME** : référentiel des métiers, garantissant un classement
  stable des offres.
* **COMPETENCE** : référentiel des compétences techniques et
  comportementales, dédoublonné.
* **OFFRE** : table de faits centrale portant les caractéristiques propres à
  chaque offre d'emploi.
* **EXIGENCE_OFFRE** : table d'association entre `OFFRE` et `COMPETENCE`,
  porteuse de l'attribut `statut_exigence`.

### Cardinalités et justifications

| Relation | Cardinalité | Justification métier |
|---|---|---|
| `COMMUNE` localise `OFFRE` | COMMUNE `1,1` — OFFRE `0,N` | Chaque offre est publiée pour un unique bassin d'emploi identifié par son code INSEE (`code_insee` est NOT NULL dans `OFFRE`) ; une commune peut ne recevoir aucune offre sur la période observée ou en recevoir plusieurs. Le code INSEE, plutôt que le code postal ou le libellé, est retenu comme clé car il est stable et sans ambiguïté, contrairement au code postal qui peut être partagé par plusieurs communes ou inversement. |
| `ENTREPRISE` publie `OFFRE` | ENTREPRISE `1,1` — OFFRE `0,N` | Chaque offre est rattachée à une entreprise recruteuse, même lorsque celle-ci choisit de rester anonyme : dans ce cas, `entreprise_id` reste renseigné mais `raison_sociale` est laissé à `NULL` et `entreprise_anonyme` passe à `true`. Une entreprise peut publier plusieurs offres ou n'en publier aucune sur le périmètre observé. Cette modélisation évite de perdre l'information de regroupement des offres d'une même entreprise anonyme tout en respectant la confidentialité demandée. |
| `METIER_ROME` catégorise `OFFRE` | METIER_ROME `1,1` — OFFRE `0,N` | Chaque offre est rattachée à un unique code ROME, permettant de comparer objectivement des offres provenant d'entreprises différentes sur un même métier ; un métier du référentiel peut ne correspondre à aucune offre observée. |
| `OFFRE` requiert `COMPETENCE` | OFFRE `0,N` — COMPETENCE `0,N`, via `EXIGENCE_OFFRE` | La relation entre une offre et les compétences attendues est intrinsèquement plusieurs-à-plusieurs : une offre mentionne généralement plusieurs compétences, et une compétence donnée (par exemple « Langage SQL ») est demandée par de nombreuses offres différentes. Une relation binaire directe entre `OFFRE` et `COMPETENCE` ne permettrait pas de conserver l'information de statut (« Exigé » ou « Souhaité ») associée à chaque paire offre/compétence ; la table de liaison `EXIGENCE_OFFRE` est donc indispensable pour porter cet attribut sans redondance et sans violer la troisième forme normale. La cardinalité minimale « une offre doit posséder au moins une compétence » ne peut pas être imposée par une simple clé étrangère dans un modèle relationnel standard ; elle est donc contrôlée en amont par le processus d'extraction et de chargement (ETL), qui rejette ou signale toute offre sans compétence extraite. |

### Gestion des cas particuliers

* **Entreprises anonymes** : l'anonymat est un attribut de l'entreprise et
  non de l'offre, car une même entreprise anonyme peut publier plusieurs
  offres sous la même identité masquée. La contrainte `CHECK` garantit qu'une
  entreprise non anonyme possède obligatoirement une raison sociale, et
  qu'une entreprise anonyme peut légitimement avoir une raison sociale
  absente.
* **Communes sans coordonnées GPS** : certaines communes du référentiel BAN
  ne disposent pas systématiquement de coordonnées précises ; `latitude` et
  `longitude` restent donc nullables, sans remettre en cause l'usage du code
  INSEE comme identifiant géographique principal.

## 5. Modélisation conceptuelle et logique

### Modèle Conceptuel des Données (description textuelle)

Une `COMMUNE`, identifiée par son code INSEE, peut localiser plusieurs
`OFFRE`. Une `ENTREPRISE`, anonyme ou non, peut publier plusieurs `OFFRE`.
Chaque `OFFRE` est rattachée à un unique `METIER_ROME` du référentiel. Chaque
`OFFRE` peut requérir plusieurs `COMPETENCE`, et chaque `COMPETENCE` peut être
requise par plusieurs `OFFRE` ; cette relation plusieurs-à-plusieurs est
portée par l'association `EXIGENCE_OFFRE`, qui qualifie chaque lien par un
statut d'exigence (« Exigé » ou « Souhaité »). Les attributs propres à
l'offre (libellé, description, date de publication, type de contrat, durée
de travail, salaire estimé) ne sont stockés qu'une seule fois, dans
`OFFRE`, car ils dépendent uniquement de l'identifiant de l'offre.

### Modèle Logique des Données

```text
COMMUNE(code_insee PK, code_postal, nom_commune, latitude, longitude)

ENTREPRISE(entreprise_id PK, raison_sociale, entreprise_anonyme)

METIER_ROME(rome_code PK, libelle_fiche_metier, domaine_professionnel)

COMPETENCE(competence_id PK, libelle_competence UQ, type_competence)

OFFRE(offre_id PK, source_offre_id UQ, libelle_poste, description,
      date_publication, type_contrat, duree_travail,
      salaire_brut_annuel_estime,
      #rome_code FK -> METIER_ROME,
      #entreprise_id FK -> ENTREPRISE,
      #code_insee FK -> COMMUNE)

EXIGENCE_OFFRE(#offre_id FK -> OFFRE, #competence_id FK -> COMPETENCE,
               statut_exigence)
PK EXIGENCE_OFFRE (offre_id, competence_id)
```

Convention de lecture : les clés primaires sont indiquées par `PK`, les
clés étrangères sont préfixées par `#` et fléchées vers la table
référencée. Le modèle respecte la troisième forme normale : chaque attribut
non-clé dépend de la totalité de la clé primaire de sa table et d'aucun
autre attribut, les référentiels (`METIER_ROME`, `COMPETENCE`, `COMMUNE`,
`ENTREPRISE`) sont séparés de la table de faits `OFFRE`, et la relation
plusieurs-à-plusieurs est résolue par `EXIGENCE_OFFRE`.

### Code DBML

Le fichier complet et directement importable dans dbdiagram.io est disponible
dans [`model.dbml`](model.dbml).

```dbml
Table commune {
  code_insee char(5) [pk, note: "Code officiel INSEE de la commune"]
  code_postal varchar(5) [not null]
  nom_commune varchar(100) [not null]
  latitude decimal(9,6)
  longitude decimal(9,6)

  indexes {
    code_postal
    nom_commune
  }
}

Table entreprise {
  entreprise_id bigint [pk, increment]
  raison_sociale varchar(150) [note: "Nullable si entreprise_anonyme = true"]
  entreprise_anonyme boolean [not null, default: false]

  indexes {
    raison_sociale
  }
}

Table metier_rome {
  rome_code varchar(5) [pk, note: "Code ROME 4.0, ex. M1805"]
  libelle_fiche_metier varchar(150) [not null]
  domaine_professionnel varchar(150) [not null]

  indexes {
    domaine_professionnel
  }
}

Table competence {
  competence_id bigint [pk, increment]
  libelle_competence varchar(150) [not null, unique]
  type_competence varchar(20) [not null, note: "Savoir-faire ou Savoir-être"]
}

Table offre {
  offre_id bigint [pk, increment]
  source_offre_id varchar(20) [not null, unique, note: "Identifiant de l'offre côté API France Travail"]
  libelle_poste varchar(200) [not null]
  description text
  date_publication date [not null]
  type_contrat varchar(5) [not null, note: "CDI, CDD, MIS, SAI, CCE"]
  duree_travail varchar(100)
  salaire_brut_annuel_estime decimal(10,2) [note: "Valeur médiane extraite du texte libre de salaire"]
  rome_code varchar(5) [not null]
  entreprise_id bigint [not null]
  code_insee char(5) [not null]

  indexes {
    rome_code
    type_contrat
    code_insee
    date_publication
    (code_insee, type_contrat)
  }
}

Table exigence_offre {
  offre_id bigint [pk]
  competence_id bigint [pk]
  statut_exigence varchar(1) [not null, note: "E = Exigé, S = Souhaité"]
}

Ref: offre.rome_code > metier_rome.rome_code
Ref: offre.entreprise_id > entreprise.entreprise_id
Ref: offre.code_insee > commune.code_insee
Ref: exigence_offre.offre_id > offre.offre_id
Ref: exigence_offre.competence_id > competence.competence_id
```

## 6. Script d'implémentation PostgreSQL

Le script complet, commenté et exécutable est disponible dans
[`sql/emploi_postgresql.sql`](sql/emploi_postgresql.sql). Il comprend, dans
l'ordre :

1. Les instructions `DROP TABLE IF EXISTS ... CASCADE` dans l'ordre inverse
   des dépendances.
2. La création des six tables avec types stricts (`BIGINT` en identité,
   `VARCHAR`, `NUMERIC`, `DATE`, `BOOLEAN`, `CHAR`), les clés primaires, les
   clés étrangères avec `ON DELETE RESTRICT/CASCADE` et `ON UPDATE CASCADE`,
   ainsi que des contraintes `CHECK` de validation (format du code INSEE,
   type de contrat autorisé, salaire positif, cohérence de l'anonymat des
   entreprises).
3. Huit index B-Tree ciblant les colonnes de jointure et de filtrage les
   plus fréquentes (code ROME, type de contrat, code INSEE, date de
   publication, couple commune/contrat, compétence).
4. Un jeu de données de test cohérent : deux communes, deux entreprises
   (dont une anonyme), deux fiches ROME, quatre compétences, trois offres et
   leurs liaisons de compétences avec statut d'exigence.
5. Deux requêtes SQL avancées orientées tableau de bord RH.

### Aperçu des requêtes d'analyse

```sql
-- Requête 1 : compétences attendues par offre, avec métier ROME,
-- type de contrat et bassin d'emploi (STRING_AGG).
SELECT
    o.source_offre_id,
    o.libelle_poste,
    mr.libelle_fiche_metier,
    o.type_contrat,
    nc.nom_commune,
    STRING_AGG(
        c.libelle_competence || ' (' || eo.statut_exigence || ')',
        ', ' ORDER BY eo.statut_exigence, c.libelle_competence
    ) AS competences_attendues
FROM offre o
JOIN metier_rome mr    ON mr.rome_code = o.rome_code
JOIN commune nc        ON nc.code_insee = o.code_insee
JOIN exigence_offre eo ON eo.offre_id = o.offre_id
JOIN competence c      ON c.competence_id = eo.competence_id
GROUP BY o.offre_id, o.source_offre_id, o.libelle_poste,
         mr.libelle_fiche_metier, o.type_contrat, nc.nom_commune
ORDER BY o.date_publication DESC;
```

```sql
-- Requête 2 : tension du marché par bassin d'emploi et type de contrat,
-- avec salaire moyen estimé (AVG) et compétences exigées associées.
SELECT
    c.nom_commune,
    o.type_contrat,
    COUNT(DISTINCT o.offre_id)                       AS nombre_offres,
    ROUND(AVG(o.salaire_brut_annuel_estime), 2)      AS salaire_moyen_estime,
    STRING_AGG(DISTINCT comp.libelle_competence, ', ' ORDER BY comp.libelle_competence)
                                                       AS competences_demandees
FROM offre o
JOIN commune c              ON c.code_insee = o.code_insee
JOIN exigence_offre eo       ON eo.offre_id = o.offre_id
JOIN competence comp         ON comp.competence_id = eo.competence_id
WHERE eo.statut_exigence = 'E'
GROUP BY c.nom_commune, o.type_contrat
ORDER BY nombre_offres DESC, salaire_moyen_estime DESC;
```

## 7. Cartographie globale du pipeline de données

```text
API France Travail (JSON) --------------+
                                          +--> Zone brute immuable, horodatée
Référentiel ROME 4.0 (CSV) --------------+
Base Adresse Nationale / INSEE (CSV) ----+
                                                    |
                                                    v
                        Audit qualité et nettoyage (data wrangling)
    (aplatissement du tableau competences[], parsing regex du salaire
     en texte libre, dédoublonnage des compétences, traitement explicite
     des entreprises anonymes, rapprochement des codes ROME et INSEE)
                                                    |
                                                    v
                    Normalisation et structuration en 3NF
   (commune, entreprise, metier_rome, competence, offre, exigence_offre)
                                                    |
                                                    v
                        Stockage PostgreSQL contraint et indexé
                                                    |
                          +-------------------------+-------------------------+
                          v                                                   v
                Exploitation BI / Metabase                          Pilotage RH et territorial
        tableaux de bord de tension par métier               détection des bassins en tension,
        et par bassin d'emploi                                aide au sourcing de compétences
```

### Détail des quatre étapes

1. **Extraction** : interroger périodiquement l'API « Offres d'emploi v2 »
   de France Travail (authentification OAuth2), et télécharger les
   millésimes du référentiel ROME 4.0 et de la Base Adresse Nationale /
   référentiel INSEE. Les réponses JSON et les fichiers CSV bruts sont
   conservés tels quels, avec horodatage de collecte, dans une zone de
   stockage brute non modifiée.

2. **Audit et nettoyage (data wrangling)** : profiler la présence ou
   l'absence des objets imbriqués (`entreprise`, `lieuTravail`, `salaire`) ;
   aplatir le tableau `competences[]` en une ligne par compétence et par
   offre ; extraire une valeur numérique de salaire à partir du texte libre
   par expression régulière (par exemple `Annuel de 38000.0 Euros à 45000.0
   Euros` devient une valeur médiane estimée) ; dédupliquer les libellés de
   compétences afin d'éviter la création de doublons dans le référentiel ;
   traiter explicitement le cas des entreprises anonymes en distinguant
   absence de nom et confidentialité volontaire ; rapprocher chaque code
   ROME et chaque code INSEE présent dans les offres avec les référentiels
   téléchargés, et journaliser les rejets en cas d'absence de correspondance.

3. **Normalisation et structuration** : construire les tables de
   référentiel (`commune`, `entreprise`, `metier_rome`, `competence`)
   dédupliquées, puis alimenter la table de faits `offre` en ne conservant
   qu'une ligne par identifiant source d'offre, et enfin peupler la table
   d'association `exigence_offre` afin de représenter la relation
   plusieurs-à-plusieurs entre offres et compétences, sans redondance et en
   respectant la troisième forme normale.

4. **Stockage et exploitation** : charger l'ensemble en transaction dans
   PostgreSQL, vérifier la satisfaction de toutes les contraintes
   d'intégrité et l'efficacité des index de jointure, puis exposer les
   données à des outils de restitution tels que Metabase ou tout autre
   outil de Business Intelligence pour construire des tableaux de bord de
   pilotage RH : compétences les plus demandées par métier et par bassin
   d'emploi, salaire moyen estimé par type de contrat, et suivi dans le
   temps des tensions de recrutement territoriales.

## Exécution

Depuis une base PostgreSQL de test nommée `emploi` :

```bash
psql -d emploi -f sql/emploi_postgresql.sql
```

Le script ne dépend d'aucune extension PostgreSQL et peut être rejoué grâce
aux instructions `DROP TABLE IF EXISTS ... CASCADE`.

### Connexion depuis Python / PyCharm

La base créée dans pgAdmin doit être accessible sur le serveur PostgreSQL
correspondant. Installer le pilote dans l'environnement virtuel :

```bash
python -m pip install -r requirements.txt
```

Configurer ensuite les variables de connexion à partir de `.env.example`.
PowerShell permet par exemple de les définir pour la session courante :

```powershell
$env:PGHOST = "localhost"
$env:PGPORT = "5432"
$env:PGDATABASE = "emploi"
$env:PGUSER = "postgres"
$env:PGPASSWORD = "votre_mot_de_passe"
python main.py
```

Le mot de passe reste local et n'est pas écrit dans le dépôt. Le script
vérifie la connexion en affichant la base et l'utilisateur PostgreSQL
effectivement connectés.

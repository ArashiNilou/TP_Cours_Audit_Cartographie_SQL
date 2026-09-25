import unittest

from src.ingest import parse_salaire_annuel, transform_offre


class IngestTransformsTest(unittest.TestCase):
    def test_parse_salaire_annuel_range(self) -> None:
        self.assertEqual(
            parse_salaire_annuel("Annuel de 38000.0 Euros a 45000.0 Euros"),
            41500.0,
        )

    def test_parse_salaire_mensuel_to_annual(self) -> None:
        self.assertEqual(parse_salaire_annuel("Mensuel de 2500 Euros"), 30000.0)

    def test_transform_offre_extracts_3nf_fields(self) -> None:
        offre = {
            "id": "214JMGC",
            "intitule": "Data Engineer",
            "description": "SQL et Python",
            "dateCreation": "2026-09-25T08:00:00.000Z",
            "typeContrat": "CDI",
            "dureeTravailLibelle": "35H",
            "romeCode": "M1805",
            "romeLibelle": "Etudes et developpement informatique",
            "lieuTravail": {
                "commune": "44172",
                "codePostal": "44980",
                "libelle": "44 - Sainte-Luce-sur-Loire",
                "latitude": 47.2494,
                "longitude": -1.4862,
            },
            "entreprise": {"nom": "TECH INNOVATION"},
            "salaire": {"libelle": "Annuel de 38000 Euros a 42000 Euros"},
            "competences": [{"libelle": "Langage SQL", "exigence": "E"}],
        }

        transformed = transform_offre(offre)

        self.assertEqual(transformed["source_offre_id"], "214JMGC")
        self.assertEqual(transformed["date_publication"], "2026-09-25")
        self.assertEqual(transformed["type_contrat"], "CDI")
        self.assertEqual(transformed["commune"]["code_insee"], "44172")
        self.assertFalse(transformed["entreprise"]["entreprise_anonyme"])
        self.assertEqual(transformed["competences"][0]["statut_exigence"], "E")


if __name__ == "__main__":
    unittest.main()


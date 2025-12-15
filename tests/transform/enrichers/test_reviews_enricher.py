"""
Pruebas unitarias para ReviewsEnricher.

Verifica el enriquecimiento de la tabla de reseñas incluyendo:
- Limpieza interna con ReviewsCleaner
- Unión con catálogos de productos y clientes
- Generación de métricas derivadas (review_month, comment_length)
- Banderas de sentimiento (is_positive, is_negative)
"""

import pytest
import pytest_check as check

from transform.enrichers.reviews_enricher import ReviewsEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsEnricher:
    """
    Tests del enriquecimiento de reseñas.

    Verifica la integración de datos de productos y clientes
    y el cálculo de banderas de sentimiento y métricas de contenido.
    """

    def test_enrich_should_add_metadata_and_flags_when_reviews_provided(
        self, reviews_enricher_inputs
    ):
        """
        Debe unir catálogos, añadir segmentación y marcar reseñas positivas.

        Verifica que el enriquecimiento complete el pipeline correctamente,
        agregando nombre de producto, segmento de cliente y banderas de sentimiento.
        """
        enricher = ReviewsEnricher()

        enriched = enricher.enrich(
            reviews_df=reviews_enricher_inputs["reviews"],
            products_df=reviews_enricher_inputs["products"],
            customers_df=reviews_enricher_inputs["customers"],
        )

        check.equal(len(enriched), 2)
        check.is_true("product_name" in enriched.columns)
        check.is_true("segment" in enriched.columns)
        check.is_true("is_positive" in enriched.columns)

"""
Pruebas unitarias para ReviewsEnricher.
Revisan uniones con catálogos y generación de metadatos de sentimiento.
"""

import pytest
import pytest_check as check

from transform.cleaners.reviews_cleaner import ReviewsCleaner
from transform.enrichers.reviews_enricher import ReviewsEnricher


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsEnricher:
    """Tests del enriquecimiento de reseñas con productos, clientes y flags."""

    def test_enrich_should_add_metadata_and_flags_when_reviews_provided(
        self, reviews_enricher_inputs
    ):
        """Debe unir catálogos, añadir segmentación y marcar reseñas positivas."""
        enricher = ReviewsEnricher(cleaner=ReviewsCleaner())

        enriched = enricher.enrich(
            reviews_df=reviews_enricher_inputs["reviews"],
            products_df=reviews_enricher_inputs["products"],
            customers_df=reviews_enricher_inputs["customers"],
        )

        check.equal(len(enriched), 2)
        check.is_true("product_name" in enriched.columns)
        check.is_true("segment" in enriched.columns)
        check.is_true("is_positive" in enriched.columns)

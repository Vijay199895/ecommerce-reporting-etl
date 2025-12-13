"""
Pruebas unitarias para ReviewsCleaner.
Validan manejo de nulos, conversiones y pipeline de limpieza.
"""

import pytest
import pytest_check as check
import pandas as pd

from transform.cleaners.reviews_cleaner import ReviewsCleaner


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsCleanerNulls:
    """Tests de `handle_nulls` para eliminar reseñas inválidas y rellenar votos."""

    def test_handle_nulls_should_drop_missing_keys_and_fill_votes_when_cleaning_reviews(
        self, raw_reviews_dirty
    ):
        """Debe descartar filas sin claves y rellenar `helpful_votes` nulos."""
        cleaner = ReviewsCleaner()

        cleaned = cleaner.handle_nulls(raw_reviews_dirty.copy())

        check.equal(len(cleaned), 1)  # drop rows missing required fields
        check.equal(cleaned["helpful_votes"].isna().sum(), 0)


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsCleanerConvert:
    """Tests de `convert_types` para normalizar calificaciones y fechas."""

    def test_convert_types_should_normalize_rating_and_date_when_reviews_dirty(
        self, raw_reviews_dirty
    ):
        """Debe convertir `rating` a numérico y `created_at` a datetime."""
        cleaner = ReviewsCleaner()

        converted = cleaner.convert_types(raw_reviews_dirty.copy())

        check.is_true(pd.api.types.is_numeric_dtype(converted["rating"]))
        check.is_true(pd.api.types.is_datetime64_any_dtype(converted["created_at"]))


@pytest.mark.unit
@pytest.mark.transform
class TestReviewsCleanerCleanPipeline:
    """Tests del método `clean` asegurando presencia de identificadores requeridos."""

    def test_clean_should_return_required_identifiers_when_pipeline_runs(
        self, raw_reviews_dirty
    ):
        """Debe devolver reseñas con `review_id`, `product_id` y `customer_id` completos."""
        cleaner = ReviewsCleaner()

        cleaned = cleaner.clean(raw_reviews_dirty)

        check.is_true(
            (cleaned[["review_id", "product_id", "customer_id"]].notna().all()).all()
        )

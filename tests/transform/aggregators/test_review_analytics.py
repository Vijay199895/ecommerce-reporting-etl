"""
Pruebas unitarias para ReviewAnalyticsAggregator.
Evalúan métricas generales, por producto y series mensuales de reseñas.
"""

import pytest
import pytest_check as check

from transform.aggregators.review_analytics import ReviewAnalyticsAggregator


@pytest.mark.unit
@pytest.mark.transform
class TestReviewAnalyticsOverview:
    """Tests de `rating_overview` para promedios y tasas positivas globales."""

    def test_rating_overview_should_compute_mean_and_positive_rate_when_reviews_provided(
        self, reviews_enriched_sample
    ):
        """Debe calcular promedio de rating y tasa positiva coherente con la muestra."""
        agg = ReviewAnalyticsAggregator()

        overview = agg.rating_overview(reviews_enriched_sample)

        check.equal(len(overview), 1)
        check.almost_equal(
            float(overview["average_rating"].iloc[0]),
            reviews_enriched_sample["rating"].mean(),
            rel=1e-3,
        )
        check.almost_equal(
            float(overview["positive_rate"].iloc[0]),
            reviews_enriched_sample["is_positive"].mean(),
            rel=1e-3,
        )


@pytest.mark.unit
@pytest.mark.transform
class TestReviewAnalyticsByProduct:
    """Tests de `rating_by_product` con mínimo de reseñas y ranking."""

    def test_rating_by_product_should_filter_min_reviews_and_rank_when_reviews_provided(
        self, reviews_enriched_sample
    ):
        """Debe filtrar por mínimo de reseñas y devolver ranking de productos."""
        agg = ReviewAnalyticsAggregator()

        ranked = agg.rating_by_product(reviews_enriched_sample, min_reviews=2, top_n=5)

        check.is_true((ranked["review_count"] >= 2).all())
        check.greater_equal(len(ranked), 2)


@pytest.mark.unit
@pytest.mark.transform
class TestReviewAnalyticsMonthly:
    """Tests de `monthly_review_volume` generando periodo y conteos acumulados."""

    def test_monthly_review_volume_should_derive_period_and_sum_when_reviews_provided(
        self, reviews_enriched_sample
    ):
        """Debe derivar el mes de reseña y sumar volúmenes en orden temporal."""
        agg = ReviewAnalyticsAggregator()

        monthly = agg.monthly_review_volume(reviews_enriched_sample)

        check.is_true(monthly["review_month"].is_monotonic_increasing)
        check.equal(monthly["volume"].sum(), len(reviews_enriched_sample))

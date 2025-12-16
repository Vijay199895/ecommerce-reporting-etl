"""
Métricas de calidad y volumen de reviews.
"""

import pandas as pd

from utils.logger import transform_logger, log_table_processing


class ReviewAnalyticsAggregator:
    """
    Agrega indicadores de satisfacción y volumen a partir de reseñas enriquecidas.
    """

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_reviews"
    )
    def rating_overview(self, enriched_reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula rating promedio, tasas de reseñas positivas y negativas y el volumen total.

        Args:
            enriched_reviews_df: DataFrame de reseñas enriquecidas
        """
        overview = pd.DataFrame(
            {
                "average_rating": [enriched_reviews_df["rating"].mean()],
                "positive_rate": [enriched_reviews_df["is_positive"].mean()],
                "negative_rate": [enriched_reviews_df["is_negative"].mean()],
                "review_count": [len(enriched_reviews_df)],
            }
        )
        return overview

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_reviews"
    )
    def rating_by_product(
        self, enriched_reviews_df: pd.DataFrame, min_reviews: int = 5, top_n: int = 20
    ) -> pd.DataFrame:
        """
        Genera ranking de productos por rating promedio, filtrando los que superan un mínimo
        de reseñas, incluye tasa positiva y devuelve el top solicitado.

        Args:
            enriched_reviews_df: DataFrame de reseñas enriquecidas
            min_reviews: Mínimo de reseñas para incluir producto (default: 5)
            top_n: Número de productos a retornar (default: 20)
        """
        grouped = (
            enriched_reviews_df.groupby("product_id")
            .agg(
                product_name=("product_name", "first"),
                average_rating=("rating", "mean"),
                review_count=("review_id", "count"),
                positive_rate=("is_positive", "mean"),
            )
            .reset_index()
        )
        filtered = grouped[grouped["review_count"] >= min_reviews]
        ranked = filtered.sort_values(
            ["average_rating", "review_count"], ascending=[False, False]
        ).head(top_n)
        return ranked

    @log_table_processing(
        stage="aggregate", logger=transform_logger, table_name="enriched_reviews"
    )
    def monthly_review_volume(self, enriched_reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega volumen de reseñas y rating promedio por mes.

        Args:
            enriched_reviews_df: DataFrame de reseñas enriquecidas
        """
        grouped = (
            enriched_reviews_df.groupby("review_month")
            .agg(volume=("review_id", "count"), average_rating=("rating", "mean"))
            .reset_index()
            .sort_values("review_month")
        )
        return grouped

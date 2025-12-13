"""
Métricas de calidad y volumen de reviews.
"""

import pandas as pd

from utils.logger import transform_logger


class ReviewAnalyticsAggregator:
    """
    Agrega indicadores de satisfacción y volumen a partir de reseñas enriquecidas.
    """

    def __init__(self, logger=transform_logger):
        self.logger = logger

    def rating_overview(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula rating promedio, tasas de reseñas positivas y negativas y el volumen total;
        devuelve un resumen de una fila o vacío si no hay datos.
        """
        if reviews_df.empty:
            return pd.DataFrame()
        overview = pd.DataFrame(
            {
                "average_rating": [
                    reviews_df["rating"].mean()
                    if "rating" in reviews_df.columns
                    else pd.NA
                ],
                "positive_rate": [
                    reviews_df["is_positive"].mean()
                    if "is_positive" in reviews_df.columns
                    else 0.0
                ],
                "negative_rate": [
                    reviews_df["is_negative"].mean()
                    if "is_negative" in reviews_df.columns
                    else 0.0
                ],
                "review_count": [len(reviews_df)],
            }
        )
        self.logger.info("Overview de reviews calculado")
        return overview

    def rating_by_product(
        self, reviews_df: pd.DataFrame, min_reviews: int = 5, top_n: int = 20
    ) -> pd.DataFrame:
        """
        Genera ranking de productos por rating promedio, filtrando los que superan un mínimo
        de reseñas, incluye tasa positiva cuando está disponible y devuelve el top solicitado.
        """
        if reviews_df.empty:
            return pd.DataFrame()
        group_cols = [
            c for c in ["product_id", "product_name"] if c in reviews_df.columns
        ]
        if not group_cols:
            group_cols = ["product_id"]
        grouped = (
            reviews_df.groupby(group_cols)
            .agg(
                average_rating=("rating", "mean"),
                review_count=("review_id", "count"),
                positive_rate=("is_positive", "mean")
                if "is_positive" in reviews_df.columns
                else ("rating", lambda s: (s >= 4).mean()),
            )
            .reset_index()
        )
        filtered = grouped[grouped["review_count"] >= min_reviews]
        ranked = filtered.sort_values(
            ["average_rating", "review_count"], ascending=[False, False]
        ).head(top_n)
        self.logger.info(
            "Ranking de productos con %s mínimos generado: %s filas",
            min_reviews,
            len(ranked),
        )
        return ranked

    def monthly_review_volume(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega volumen de reseñas y rating promedio por mes; si falta `review_month`, lo deriva
        desde `created_at`, devolviendo la serie temporal ordenada.
        """
        if reviews_df.empty:
            return pd.DataFrame()
        df = reviews_df.copy()
        if "review_month" not in df.columns and "created_at" in df.columns:
            df["review_month"] = df["created_at"].dt.to_period("M")
        grouped = (
            df.groupby("review_month")
            .agg(volume=("review_id", "count"), average_rating=("rating", "mean"))
            .reset_index()
            .sort_values("review_month")
        )
        self.logger.info(
            "Volumen mensual de reviews calculado: %s periodos", len(grouped)
        )
        return grouped

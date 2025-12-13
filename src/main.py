"""
Punto de entrada principal para el proceso ETL.

Orquesta las etapas de extracción, transformación y carga.
"""

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from extract.csv_extractor import CSVExtractor
from transform.cleaners.orders_cleaner import OrdersCleaner
from transform.cleaners.inventory_cleaner import InventoryCleaner
from transform.cleaners.reviews_cleaner import ReviewsCleaner
from transform.enrichers.orders_enricher import OrdersEnricher
from transform.enrichers.inventory_enricher import InventoryEnricher
from transform.enrichers.reviews_enricher import ReviewsEnricher
from transform.aggregators.customer_analytics import CustomerAnalyticsAggregator
from transform.aggregators.product_analytics import ProductAnalyticsAggregator
from transform.aggregators.sales_analytics import SalesAnalyticsAggregator
from transform.aggregators.inventory_analytics import InventoryAnalyticsAggregator
from transform.aggregators.review_analytics import ReviewAnalyticsAggregator
from transform.aggregators.order_lifecycle import OrderLifecycleAggregator
from utils.logger import extract_logger, transform_logger, load_logger


RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def extract_stage(raw_dir: Path) -> Dict[str, pd.DataFrame]:
    """Extrae los datasets necesarios desde CSV."""

    def _extract(filename: str, parse_dates: Optional[list] = None) -> pd.DataFrame:
        extractor = CSVExtractor(
            file_path=raw_dir / filename,
            parse_dates=parse_dates or [],
            auto_profile=False,
        )
        return extractor.extract()

    extract_logger.info("Iniciando extracción desde %s", raw_dir)

    tables = {
        "orders": ("ecommerce_orders.csv", ["order_date"]),
        "order_items": ("ecommerce_order_items.csv", None),
        "customers": ("ecommerce_customers.csv", ["registration_date"]),
        "promotions": ("ecommerce_promotions.csv", None),
        "products": ("ecommerce_products.csv", None),
        "categories": ("ecommerce_categories.csv", None),
        "brands": ("ecommerce_brands.csv", None),
        "reviews": ("ecommerce_reviews.csv", ["created_at"]),
        "inventory": ("ecommerce_inventory.csv", None),
        "warehouses": ("ecommerce_warehouses.csv", None),
    }

    for key, (filename, parse_dates) in tables.items():
        tables[key] = _extract(filename, parse_dates=parse_dates)

    return tables


def transform_stage(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Limpia y enriquece datasets disponibles."""

    # Órdenes
    orders_cleaner = OrdersCleaner()
    orders_enricher = OrdersEnricher(orders_cleaner)
    enriched_orders = orders_enricher.enrich(
        orders_df=tables["orders"],
        customers_df=tables["customers"],
        promotions_df=tables["promotions"],
        order_items_df=tables["order_items"]
    )

    # Inventario
    inventory_cleaner = InventoryCleaner()
    inventory_enricher = InventoryEnricher(inventory_cleaner)
    enriched_inventory = inventory_enricher.enrich(
        inventory_df=tables["inventory"],
        products_df=tables["products"],
        warehouses_df=tables["warehouses"],
    )

    # Reviews
    reviews_cleaner = ReviewsCleaner()
    reviews_enricher = ReviewsEnricher(reviews_cleaner)
    enriched_reviews = reviews_enricher.enrich(
        reviews_df=tables["reviews"],
        products_df=tables["products"],
        customers_df=tables["customers"],
    )

    return {
        "orders": enriched_orders,
        "inventory": enriched_inventory,
        "reviews": enriched_reviews,
    }


def aggregate_stage(
    enriched: Dict[str, pd.DataFrame], tables: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:
    """Genera métricas de negocio a partir de datasets enriquecidos."""

    enriched_orders = enriched["orders"]
    enriched_inventory = enriched["inventory"]
    enriched_reviews = enriched["reviews"]

    customer_agg = CustomerAnalyticsAggregator()
    product_agg = ProductAnalyticsAggregator()
    sales_agg = SalesAnalyticsAggregator()
    inventory_agg = InventoryAnalyticsAggregator()
    review_agg = ReviewAnalyticsAggregator()
    lifecycle_agg = OrderLifecycleAggregator()

    results = {
        "top_spenders": customer_agg.top_spenders(
            enriched_orders, top_n=5, percentile=0.8
        ),
        "recurring_customers": customer_agg.recurring_customers(
            enriched_orders, min_orders=2
        ),
        "average_ticket": pd.DataFrame(
            {"average_ticket": [customer_agg.average_ticket_overall(enriched_orders)]}
        ),
        "top_products": product_agg.top_products_by_quantity(
            order_items_df=tables["order_items"],
            products_df=tables.get("products"),
            top_n=10,
        ),
        "monthly_sales": sales_agg.monthly_sales(enriched_orders),
        "promotion_usage_rate": pd.DataFrame(
            {"promotion_usage_rate": [sales_agg.promotion_usage_rate(enriched_orders)]}
        ),
        "status_funnel": lifecycle_agg.status_funnel(enriched_orders),
        "cancellation_rate": pd.DataFrame(
            {"cancellation_rate": [lifecycle_agg.cancellation_rate(enriched_orders)]}
        ),
        "delivery_rate": pd.DataFrame(
            {"delivery_rate": [lifecycle_agg.delivery_rate(enriched_orders)]}
        ),
        "backlog_in_progress": lifecycle_agg.in_progress_backlog(enriched_orders),
        "inventory_health": inventory_agg.stock_health_summary(enriched_inventory),
        "low_stock_items": inventory_agg.low_stock_items(enriched_inventory, top_n=20),
        "warehouse_utilization": inventory_agg.warehouse_utilization(
            enriched_inventory
        ),
        "reviews_overview": review_agg.rating_overview(enriched_reviews),
        "reviews_by_product": review_agg.rating_by_product(
            enriched_reviews, min_reviews=3, top_n=20
        ),
        "reviews_monthly": review_agg.monthly_review_volume(enriched_reviews),
    }

    return results


def preview_results(results: Dict[str, pd.DataFrame]) -> None:
    """Imprime en consola una vista corta de cada resultado."""

    for name, df in results.items():
        transform_logger.info("Resultado '%s' (primeras filas):", name)
        print("\n===", name, "===")
        print(df.head())


def main() -> None:
    """Orquesta el flujo ETL (sin carga persistente)."""

    tables = extract_stage(RAW_DATA_DIR)
    enriched = transform_stage(tables)
    results = aggregate_stage(enriched, tables)

    preview_results(results)

    # TODO: carga (ejemplo futuro)
    # from load.parquet_loader import ParquetLoader
    # base_dir = Path(__file__).resolve().parent.parent / "data"
    # processed_dir = base_dir / "processed"
    # output_dir = base_dir / "output"
    # loader_processed = ParquetLoader(output_dir=processed_dir, logger=load_logger)
    # loader_output = ParquetLoader(output_dir=output_dir, logger=load_logger)
    # loader_processed.save(enriched_orders, "orders_enriched")
    # for name, df in results.items():
    #     loader_output.save(df, name)


if __name__ == "__main__":
    main()

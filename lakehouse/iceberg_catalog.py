"""
Apache Iceberg Catalog for Foxa Lakehouse
Manages table metadata and schema evolution.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from loguru import logger

# PyIceberg imports
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, DoubleType, StringType, TimestampType, 
    DateType, BooleanType, NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform, DayTransform
from pyiceberg.table import Table
import pyiceberg.expressions as exp

from lakehouse.minio_client import get_minio_client


class IcebergCatalog:
    """
    Manages Iceberg tables with SQLite-backed catalog.
    Tables stored in MinIO (S3-compatible).
    """
    
    def __init__(
        self,
        catalog_name: str = "foxa_catalog",
        warehouse_path: str = "s3://foxa-warehouse/",
        sqlite_path: Optional[str] = None
    ):
        self.catalog_name = catalog_name
        self.warehouse_path = warehouse_path
        
        # SQLite catalog for metadata
        if sqlite_path is None:
            sqlite_path = str(Path(__file__).parent.parent / "data" / "iceberg_catalog.db")
        
        self.sqlite_path = sqlite_path
        self._catalog: Optional[SqlCatalog] = None
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.sqlite_path), exist_ok=True)
        
        self._init_catalog()
    
    def _init_catalog(self):
        """Initialize the SQLite-backed Iceberg catalog."""
        try:
            self._catalog = SqlCatalog(
                self.catalog_name,
                **{
                    "uri": f"sqlite:///{self.sqlite_path}",
                    "warehouse": self.warehouse_path,
                }
            )
            logger.info(f"Iceberg catalog initialized: {self.sqlite_path}")
        except Exception as e:
            logger.error(f"Failed to initialize catalog: {e}")
            raise
    
    def create_table(
        self,
        table_name: str,
        schema: Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> Optional[Table]:
        """Create a new Iceberg table."""
        try:
            if self._catalog is None:
                self._init_catalog()
            
            # Check if table exists
            if self.table_exists(table_name):
                logger.info(f"Table {table_name} already exists")
                return self._catalog.load_table(table_name)
            
            # Create table
            table = self._catalog.create_table(
                identifier=table_name,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties or {}
            )
            
            logger.success(f"Created Iceberg table: {table_name}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return None
    
    def load_table(self, table_name: str) -> Optional[Table]:
        """Load an existing Iceberg table."""
        try:
            if self._catalog is None:
                self._init_catalog()
            return self._catalog.load_table(table_name)
        except Exception as e:
            logger.error(f"Failed to load table {table_name}: {e}")
            return None
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the catalog."""
        try:
            if self._catalog is None:
                self._init_catalog()
            self._catalog.load_table(table_name)
            return True
        except:
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """Drop a table from the catalog."""
        try:
            if self._catalog is None:
                self._init_catalog()
            self._catalog.drop_table(table_name)
            logger.info(f"Dropped table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            return False
    
    def list_tables(self, namespace: str = "default") -> List[str]:
        """List all tables in a namespace."""
        try:
            if self._catalog is None:
                self._init_catalog()
            tables = self._catalog.list_tables(namespace)
            return [str(t) for t in tables]
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def create_namespace(self, namespace: str) -> bool:
        """Create a namespace (database)."""
        try:
            if self._catalog is None:
                self._init_catalog()
            self._catalog.create_namespace(namespace)
            logger.info(f"Created namespace: {namespace}")
            return True
        except Exception as e:
            logger.warning(f"Namespace creation failed (may exist): {e}")
            return False


# ============================================================
# Schema Definitions
# ============================================================

class TableSchemas:
    """Pre-defined schemas for Foxa Lakehouse tables."""
    
    @staticmethod
    def ohlcv_schema() -> Schema:
        """Schema for OHLCV market data."""
        return Schema(
            NestedField(1, "symbol", StringType(), required=True),
            NestedField(2, "date", DateType(), required=True),
            NestedField(3, "open", DoubleType(), required=True),
            NestedField(4, "high", DoubleType(), required=True),
            NestedField(5, "low", DoubleType(), required=True),
            NestedField(6, "close", DoubleType(), required=True),
            NestedField(7, "volume", LongType(), required=True),
            NestedField(8, "timestamp", TimestampType(), required=False),
            NestedField(9, "exchange", StringType(), required=False),
        )
    
    @staticmethod
    def signals_schema() -> Schema:
        """Schema for trading signals."""
        return Schema(
            NestedField(1, "symbol", StringType(), required=True),
            NestedField(2, "date", DateType(), required=True),
            NestedField(3, "strategy", StringType(), required=True),
            NestedField(4, "signal", StringType(), required=True),
            NestedField(5, "entry_price", DoubleType(), required=False),
            NestedField(6, "target_price", DoubleType(), required=False),
            NestedField(7, "stop_loss", DoubleType(), required=False),
            NestedField(8, "confidence", StringType(), required=False),
            NestedField(9, "rsi", DoubleType(), required=False),
            NestedField(10, "adx", DoubleType(), required=False),
            NestedField(11, "scan_timestamp", TimestampType(), required=True),
        )
    
    @staticmethod
    def fundamentals_schema() -> Schema:
        """Schema for fundamental data."""
        return Schema(
            NestedField(1, "symbol", StringType(), required=True),
            NestedField(2, "date", DateType(), required=True),
            NestedField(3, "market_cap", DoubleType(), required=False),
            NestedField(4, "pe_ratio", DoubleType(), required=False),
            NestedField(5, "pb_ratio", DoubleType(), required=False),
            NestedField(6, "roe", DoubleType(), required=False),
            NestedField(7, "debt_equity", DoubleType(), required=False),
            NestedField(8, "revenue_growth", DoubleType(), required=False),
            NestedField(9, "profit_growth", DoubleType(), required=False),
            NestedField(10, "source", StringType(), required=False),
            NestedField(11, "updated_at", TimestampType(), required=True),
        )
    
    @staticmethod
    def agent_analysis_schema() -> Schema:
        """Schema for AI agent analysis results."""
        return Schema(
            NestedField(1, "symbol", StringType(), required=True),
            NestedField(2, "date", DateType(), required=True),
            NestedField(3, "agent_type", StringType(), required=True),
            NestedField(4, "recommendation", StringType(), required=True),
            NestedField(5, "score", DoubleType(), required=False),
            NestedField(6, "confidence", DoubleType(), required=False),
            NestedField(7, "reasoning", StringType(), required=False),
            NestedField(8, "analysis_timestamp", TimestampType(), required=True),
        )


# ============================================================
# Partition Specifications
# ============================================================

class PartitionSpecs:
    """Pre-defined partition specs for tables."""
    
    @staticmethod
    def by_year_month(schema: Schema, date_field: str = "date") -> PartitionSpec:
        """Partition by year and month."""
        # Find the date field id
        date_field_id = None
        for field in schema.fields:
            if field.name == date_field:
                date_field_id = field.field_id
                break
        
        if date_field_id is None:
            raise ValueError(f"Date field '{date_field}' not found in schema")
        
        return PartitionSpec(
            PartitionField(date_field_id, 1000, YearTransform()),
            PartitionField(date_field_id, 1001, MonthTransform()),
        )
    
    @staticmethod
    def by_symbol(schema: Schema, symbol_field: str = "symbol") -> PartitionSpec:
        """Partition by symbol (for smaller tables)."""
        symbol_field_id = None
        for field in schema.fields:
            if field.name == symbol_field:
                symbol_field_id = field.field_id
                break
        
        if symbol_field_id is None:
            raise ValueError(f"Symbol field '{symbol_field}' not found in schema")
        
        return PartitionSpec(
            PartitionField(symbol_field_id, 1000, "identity"),
        )


# ============================================================
# Singleton Instance
# ============================================================

_catalog_instance: Optional[IcebergCatalog] = None


def get_catalog() -> IcebergCatalog:
    """Get or create the singleton catalog instance."""
    global _catalog_instance
    if _catalog_instance is None:
        _catalog_instance = IcebergCatalog()
    return _catalog_instance


def init_lakehouse_tables():
    """Initialize all standard lakehouse tables."""
    catalog = get_catalog()
    schemas = TableSchemas()
    partitions = PartitionSpecs()
    
    # Create namespaces
    catalog.create_namespace("bronze")
    catalog.create_namespace("silver")
    catalog.create_namespace("gold")
    
    # Bronze tables (raw data)
    catalog.create_table(
        "bronze.ohlcv",
        schemas.ohlcv_schema(),
        partitions.by_year_month(schemas.ohlcv_schema()),
        properties={"write_compression": "ZSTD"}
    )
    
    catalog.create_table(
        "bronze.fundamentals",
        schemas.fundamentals_schema(),
        partitions.by_symbol(schemas.fundamentals_schema()),
        properties={"write_compression": "ZSTD"}
    )
    
    # Silver tables (cleaned)
    catalog.create_table(
        "silver.ohlcv_clean",
        schemas.ohlcv_schema(),
        partitions.by_year_month(schemas.ohlcv_schema()),
        properties={"write_compression": "ZSTD"}
    )
    
    catalog.create_table(
        "silver.indicators",
        schemas.ohlcv_schema(),  # Extended with indicator columns
        partitions.by_year_month(schemas.ohlcv_schema()),
        properties={"write_compression": "ZSTD"}
    )
    
    # Gold tables (analytics)
    catalog.create_table(
        "gold.signals",
        schemas.signals_schema(),
        partitions.by_year_month(schemas.signals_schema()),
        properties={"write_compression": "ZSTD"}
    )
    
    catalog.create_table(
        "gold.agent_analysis",
        schemas.agent_analysis_schema(),
        partitions.by_year_month(schemas.agent_analysis_schema()),
        properties={"write_compression": "ZSTD"}
    )
    
    logger.success("All lakehouse tables initialized!")


if __name__ == "__main__":
    # Test the catalog
    init_lakehouse_tables()
    
    catalog = get_catalog()
    tables = catalog.list_tables()
    print(f"\nTables in catalog: {tables}")
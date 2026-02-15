"""
Lakehouse module for Foxa Trading Platform.
Provides Apache Iceberg tables on MinIO storage.
"""

from .minio_client import MinIOClient, get_minio_client
from .iceberg_catalog import IcebergCatalog, get_catalog

__all__ = [
    "MinIOClient",
    "get_minio_client",
    "IcebergCatalog", 
    "get_catalog"
]

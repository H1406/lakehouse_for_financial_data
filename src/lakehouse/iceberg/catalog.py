"""PyIceberg client for the shared REST catalog (the same catalog Spark uses as `lake`)."""

from pyiceberg.catalog import Catalog, load_catalog

from lakehouse.config import settings


def catalog() -> Catalog:
    s = settings()
    return load_catalog(
        "lake",
        type="rest",
        uri=s.iceberg_rest_uri,
        warehouse=s.warehouse,
        **{
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key.get_secret_value(),
            "s3.region": s.s3_region,
            "s3.force-virtual-addressing": "false",  # SeaweedFS needs path-style URLs
        },
    )

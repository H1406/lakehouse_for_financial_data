"""`make init` lake part: create Iceberg namespaces and tables (idempotent). Bucket creation is in the Makefile."""

from lakehouse.iceberg.catalog import catalog
from lakehouse.iceberg.tables import create_all

if __name__ == "__main__":
    for line in create_all(catalog()):
        print(line)

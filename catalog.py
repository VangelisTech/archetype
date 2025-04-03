from daft import Catalog
from pyiceberg.catalog.sql import SqlCatalog

# don't forget to `mkdir -p /tmp/daft/example`
tmpdir = "/tmp/daft/example"

# create a pyiceberg catalog backed by sqlite
iceberg_catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{tmpdir}/catalog.db",
        "warehouse": f"file://{tmpdir}",
    },
)

# creating a daft catalog from the pyiceberg catalog implementation
catalog = Catalog.from_iceberg(iceberg_catalog)

# check
catalog.name
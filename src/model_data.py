# import duckdb

# duckdb.sql("""
# INSTALL httpfs;
# INSTALL iceberg;
# LOAD httpfs;
# LOAD iceberg;
# SET s3_region = 'us-east-1';
# SET s3_access_key_id = 'admin;
# SET s3_secret_access_key = 'password';
# select *
# from iceberg_scan(
# s3://<bucket>/<iceberg-table-folder>/metadata/<id>.metadata.json)
# limit 10
# """).show()


from pyiceberg.catalog import load_catalog
catalog = load_catalog(
    "docs",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)

table = catalog.load_table("netflix.titles_raw")
con = table.scan().to_duckdb("netflix_titles_raw")
con.sql("select * from netflix_titles_raw limit 10").show()
# ingest/postal_ingest.py

import io

import backoff
import brotli
import geopandas as gpd
import requests
from shapely.geometry import MultiPolygon

from utils.config import RAW_SCHEMA, DEFAULT_PLZ3_PREFIX
from utils.db import get_psycopg_conn, ensure_postgis_extension, ensure_postal_raw_schema
from utils.logger import logger


@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_tries=3)
def download_topojson(url: str):
    """Download TopoJSON with retry logic."""
    logger.info("Downloading postal TopoJSON from %s", url)
    # Increase timeout for large file download
    r = requests.get(url, timeout=300, stream=True)  # 5 minute timeout
    r.raise_for_status()
    return r.content

def load_postal_topojson(url: str):
    """
    Download topojson.br, decompress, and upsert postal polygons into dim_postal_area.
    Returns number of features ingested.
    """
    # Download with retry logic
    content = download_topojson(url)

    # Decompress Brotli
    try:
        geojson_bytes = brotli.decompress(content)
    except Exception as e:
        logger.error("Failed to decompress TopoJSON: %s", e)
        raise

    # read into geopandas
    try:
        gdf = gpd.read_file(io.BytesIO(geojson_bytes))
        gdf = gdf[gdf['postcode'].astype(str).str.startswith(DEFAULT_PLZ3_PREFIX)]

    except Exception:
        # fallback: save to tmp and read
        tmp_file = "tmp_postal.json"
        open(tmp_file, "wb").write(geojson_bytes)
        gdf = gpd.read_file(tmp_file)

    #as per schema of topojson, PLZ key is postcode
    plz_col = "postcode"

    # normalize
    gdf = gdf[~gdf[plz_col].isna()]
    gdf['plz'] = gdf[plz_col].astype(str).str.strip()
    
    # ensure MultiPolygon, handle GeometryCollection and other types
    def convert_to_multipolygon(geom):
        if geom is None:
            return None
        if geom.geom_type == "MultiPolygon":
            return geom
        elif geom.geom_type == "Polygon":
            # Convert single polygon to multipolygon
            return MultiPolygon([geom])
        elif geom.geom_type == "GeometryCollection":
            # Extract only polygon geometries from collection
            polygons = [g for g in geom.geoms if g.geom_type in ["Polygon", "MultiPolygon"]]
            if polygons:
                # Flatten any MultiPolygons in the collection
                all_polygons = []
                for poly in polygons:
                    if poly.geom_type == "MultiPolygon":
                        all_polygons.extend(list(poly.geoms))
                    else:
                        all_polygons.append(poly)
                return MultiPolygon(all_polygons) if all_polygons else None
            else:
                return None
        else:
            # For other types, try to convert to polygon
            try:
                return geom.buffer(0).union(geom)
            except:
                return None
    
    gdf['geometry'] = gdf.geometry.apply(convert_to_multipolygon)

    # prepare WKT and filter out invalid geometries
    gdf['wkt'] = gdf.geometry.apply(lambda g: g.wkt if g is not None else None)
    
    # Filter out rows with None geometries
    gdf = gdf[gdf['geometry'].notna()]
    logger.info("After geometry conversion: %d valid geometries", len(gdf))

    # Ensure required schemas exist
    ensure_postgis_extension()
    ensure_postal_raw_schema()

    # upsert into Postgres row by row
    # This ensures only one row per PLZ exists in the table
    conn = get_psycopg_conn()
    cur = conn.cursor()
    upsert_sql = f"""
    INSERT INTO {RAW_SCHEMA}.postal_area_raw (plz, wkt, geometry, record_source)
    VALUES (%s, %s, ST_Multi(ST_GeomFromText(%s,4326)), %s)
    """
    count = 0
    for _, row in gdf.iterrows():
        plz = str(row['plz'])
        wkt = row['wkt']
        record_source = "topojson"
        try:
            cur.execute(upsert_sql, (plz, wkt, wkt, record_source))
            conn.commit()  # Commit after each successful row
            count += 1
        except Exception as e:
            logger.warning("Failed upsert for plz=%s: %s", plz, e)
            conn.rollback()  # Rollback the failed transaction
    cur.close()
    conn.close()

    logger.info("Ingested %d postal polygons", count)
    return count



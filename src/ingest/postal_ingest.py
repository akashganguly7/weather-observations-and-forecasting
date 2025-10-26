# ingest/postal_ingest.py

import io
import requests
import geopandas as gpd
import brotli
from utils.db import get_psycopg_conn, ensure_postgis_extension, ensure_postal_area_schema
from utils.config import DIMENSIONS_SCHEMA
from utils.logger import logger
from shapely.geometry import MultiPolygon


def load_postal_topojson(url: str, plz3_prefix=None,):
    """
    Download topojson.br, decompress, and upsert postal polygons into dim_postal_area.
    Returns number of features ingested.
    """
    logger.info("Downloading postal TopoJSON from %s", url)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    # Decompress Brotli
    try:
        geojson_bytes = brotli.decompress(r.content)
    except Exception as e:
        logger.error("Failed to decompress TopoJSON: %s", e)
        raise

    # read into geopandas
    try:
        gdf = gpd.read_file(io.BytesIO(geojson_bytes))
        if plz3_prefix:
            gdf = gdf[gdf['postcode'].astype(str).str.startswith(plz3_prefix)]

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
    
    # Remove duplicates by PLZ - keep the first occurrence
    logger.info("Before deduplication: %d rows", len(gdf))
    gdf = gdf.drop_duplicates(subset=['plz'], keep='first')
    logger.info("After deduplication: %d unique PLZ rows", len(gdf))

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
    ensure_postal_area_schema()

    # upsert into Postgres row by row
    # This ensures only one row per PLZ exists in the table
    conn = get_psycopg_conn()
    cur = conn.cursor()
    upsert_sql = f"""
    INSERT INTO {DIMENSIONS_SCHEMA}.dim_postal_area (plz, wkt, geometry)
    VALUES (%s, %s, ST_Multi(ST_GeomFromText(%s,4326)))
    ON CONFLICT (plz) DO UPDATE SET
      wkt = EXCLUDED.wkt,
      geometry = EXCLUDED.geometry;
    """
    count = 0
    for _, row in gdf.iterrows():
        plz = str(row['plz'])
        wkt = row['wkt']
        try:
            cur.execute(upsert_sql, (plz, wkt, wkt))
            conn.commit()  # Commit after each successful row
            count += 1
        except Exception as e:
            logger.warning("Failed upsert for plz=%s: %s", plz, e)
            conn.rollback()  # Rollback the failed transaction
    cur.close()
    conn.close()

    logger.info("Ingested %d postal polygons", count)
    return count



"""Verified feed binding: FTS for Pilot/Flying J, Synergy for Love's."""


def pilot_price_rows(fts_parameter, pilot_parameter):
    """Reusable SQL rows; parameters are fixed placeholders supplied by code.

    FTS prices retain their original feed/customer identity. A legacy direct
    Pilot quote is a fallback only when no FTS account has been configured.
    Dates and truck-access eligibility are applied by each caller.
    """
    return f"""
        SELECT fs.pilot_site_id AS site_id, fs.id AS fuel_stop_id,
               fs.station_name,fs.address,fs.city,fs.state,
               COALESCE(fs.snapped_lat,fs.latitude) AS latitude,
               COALESCE(fs.snapped_lon,fs.longitude) AS longitude,
               fs.truck_accessible,q.your_price,q.retail_price,q.effective_date,
               i.uploaded_at,'fts'::text AS price_provider,i.date_source AS price_date_source
        FROM price_feed_quotes q
        JOIN price_file_imports i ON i.id=q.import_id
        JOIN fuel_stops fs ON fs.id=q.fuel_stop_id
        WHERE q.provider='fts' AND q.account_number={fts_parameter}
          AND {fts_parameter}<>'' AND i.status IN ('completed','held')
          AND fs.pilot_site_id IS NOT NULL
          AND (fs.station_name ILIKE '%pilot%' OR fs.station_name ILIKE '%flying j%')
        UNION ALL
        SELECT cp.site_id,fs.id AS fuel_stop_id,fs.station_name,fs.address,fs.city,fs.state,
               COALESCE(fs.snapped_lat,fs.latitude) AS latitude,
               COALESCE(fs.snapped_lon,fs.longitude) AS longitude,
               fs.truck_accessible,cp.your_price,cp.retail_price,cp.effective_date,
               cp.uploaded_at,'pilot'::text AS price_provider,'supplier'::text AS price_date_source
        FROM contracted_prices cp
        JOIN fuel_stops fs ON fs.pilot_site_id = cp.site_id
        WHERE {fts_parameter}='' AND cp.account_number={pilot_parameter}
          AND (fs.station_name ILIKE '%pilot%' OR fs.station_name ILIKE '%flying j%')
    """

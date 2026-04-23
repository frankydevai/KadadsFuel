from database import db_cursor
import sys

try:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) as cnt FROM driver_flags
            WHERE flagged_at >= NOW() - INTERVAL '%s minutes'
            """,
            (60,),
        )
        print("Success:", cur.fetchone())
except Exception as e:
    print("Error:", e)

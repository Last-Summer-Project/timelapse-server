import mariadb
import logging
from dotenv import dotenv_values
from typing import Optional


class DBConn:
    conn = None

    def __init__(self):
        env: dict = dotenv_values()
        self.conn = mariadb.connect(
            user=env.get("DB_USER"),
            password=env.get("DB_PASS"),
            host=env.get("DB_HOST"),
            port=int(env.get("DB_PORT")),
            database=env.get("DB_DATABASE")
        )
        self.limit = int(env.get("DB_LIMIT", "1"))
        logging.info("DB Client is up")

    def get_not_started(self,  limit: Optional[int] = None):
        limit = self.limit if limit is None else limit
        cur = self.conn.cursor()
        cur.execute("SELECT"
                    "	tl.id"
                    " FROM timelapse AS tl"
                    " WHERE tl.status = 'not_started'"
                    " ORDER BY tl.date_created ASC"
                    " LIMIT ?", (limit,))
        if cur is None:
            return []
        r = [log_id for (log_id,) in cur]
        logging.debug(f"Got db select: {r}")
        cur.close()
        return r

    def get_image_urls(self, idx: int):
        cur = self.conn.cursor()
        cur.execute("SELECT"
                    "    img.url AS url"
                    "  FROM device_log AS log"
                    "    INNER JOIN"
                    "      timelapse AS tl"
                    "        ON"
                    "            tl.device_id = log.device_id"
                    "          AND"
                    "            log.date_created"
                    "            BETWEEN"
                    "                tl.start_date"
                    "              AND"
                    "                tl.end_date"
                    "    INNER JOIN"
                    "      image AS img"
                    "        ON log.image_id = img.id"
                    "  WHERE tl.id = ?"
                    "  ORDER BY log.date_created ASC",
                    (idx,))
        if cur is None:
            return []
        r = [url for (url,) in cur]
        logging.debug(f"Got db select: {r}")
        cur.close()
        return r

    def update_timelapse(self, log_id: int, status='in_progress', result: Optional[str] = None):
        cur = self.conn.cursor()
        try:
            logging.debug(f"Update Timelapse ID '{log_id}' with status '{status}' and result '{result}'")
            cur.execute("UPDATE timelapse"
                        " SET"
                        "  status = ?,"
                        "  result = ?"
                        " WHERE id = ?", (status, result, log_id,))
        except mariadb.Error as e:
            logging.error(f"Maria DB Error: {e}")
        finally:
            r = cur.lastrowid
            logging.debug(f"Got db update: {r}")
            cur.close()
        self.conn.commit()
        return r

    def __del__(self):
        self.conn.close()

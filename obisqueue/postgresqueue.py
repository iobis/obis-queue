from __future__ import annotations
from obisqueue import Queue, Task
import psycopg2
import os
from psycopg2.extras import DictCursor
import json
import logging
from retry import retry


logger = logging.getLogger(__name__)


class PostgresQueue(Queue):

    def get_connection(self):
        con = psycopg2.connect(
            dbname=os.getenv("DB_DB"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        return con

    def cleanup(self, queue: str="", minutes: int=0) -> None:
        con = self.get_connection()
        try:
            with con:
                with con.cursor() as cur:
                    logger.debug("Deleting completed tasks")
                    cur.execute("""
                        delete from queue
                        where queue = %s and completed_at <= now() - interval '%s minutes'
                    """, (queue, minutes))
        except Exception as e:
            logger.error(e)
        finally:
            con.close()

    def publish(self, task: Task) -> int | None:
        id = None
        con = self.get_connection()
        try:
            with con:
                with con.cursor() as cur:
                    logger.debug("Publishing task")
                    cur.execute("""
                        insert into queue (queue, priority, payload, created_at)
                        values (%s, %s, %s, now())
                        returning id
                    """, (task.queue, task.priority, json.dumps(task.payload)))
                    row = cur.fetchone()
                    if row is not None:
                        id = row[0]
        except Exception as e:
            logger.error(e)
        finally:
            con.close()
        return id

    def consume(self, queue: str, client: str, release_minutes: int) -> Task | None:
        task = None
        con = self.get_connection()
        try:
            with con:
                with con.cursor(cursor_factory=DictCursor) as cur:
                    logger.debug("Releasing expired tasks")
                    cur.execute("""
                        update queue
                        set locked_at = null, release_at = null, client = null, released_at = now()
                        where release_at < now() and completed_at is null and queue = %s
                    """, (queue,))
                    logger.debug("Consuming task")
                    cur.execute(""" 
                        update queue set
                        client = %s,
                        locked_at = now(),
                        release_at = now() + interval '%s minutes'
                        where id in (
                            select id from queue
                            where locked_at is null and queue = %s
                            order by priority asc, created_at asc
                            limit 1
                            for update skip locked
                        )
                        returning id, queue, priority, payload, locked_at
                    """, (client, release_minutes, queue))
                    row = cur.fetchone()    
                    if row is not None:
                        task = Task(id=row["id"], queue=row["queue"], priority=row["priority"], payload=row["payload"], locked_at=row["locked_at"])
        except Exception as e:
            logger.error(e)
        finally:
            con.close()
        return task

    @retry(tries=3, delay=60, backoff=2, logger=logger)
    def complete(self, task_id: int) -> None:
        con = self.get_connection()
        try:
            with con:
                with con.cursor() as cur:
                    logger.debug("Completing task")
                    cur.execute("""
                        update queue
                        set completed_at = now()
                        where id = %s
                    """, (task_id,))
        except Exception as e:
            logger.error(e)
        finally:
            con.close()

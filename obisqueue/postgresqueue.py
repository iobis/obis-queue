from obisqueue import Queue, Task
from psycopg2 import pool
import os
from psycopg2.extras import DictCursor
import json


class PostgresQueue(Queue):

    connection_pool = None

    def __init__(self):
        if PostgresQueue.connection_pool is None: 
            PostgresQueue.connection_pool = pool.SimpleConnectionPool(
                1, 10,
                dbname=os.getenv("DB_DB"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST")
            )

    def cleanup(self) -> None:
        con = PostgresQueue.connection_pool.getconn()
        cur = con.cursor()
        cur.execute("""
            update queue
            set locked_at = null, release_at = null, client = null, released_at = now()
            where release_at < now() and completed_at is null
        """)
        cur.execute("""
            delete from queue
            where completed_at is not null
        """)
        con.commit()
        cur.close()
        PostgresQueue.connection_pool.putconn(con)

    def publish(self, task: Task) -> int:
        con = PostgresQueue.connection_pool.getconn()
        cur = con.cursor()
        cur.execute("""
            insert into queue (queue, priority, payload, created_at)
            values (%s, %s, %s, now())
            returning id
        """, (task.queue, task.priority, json.dumps(task.payload)))
        id = cur.fetchone()[0]
        con.commit()
        cur.close()
        PostgresQueue.connection_pool.putconn(con)
        return id

    def consume(self, queue: str, client: str, release_minutes: int) -> Task:
        self.cleanup()
        con = PostgresQueue.connection_pool.getconn()
        con.autocommit = False
        cur = con.cursor(cursor_factory=DictCursor)
        cur.execute("""
            select id, queue, priority, payload, created_at
            from queue
            where locked_at is null and queue = %s
            order by priority asc, created_at asc
            limit 1 for update skip locked
        """, (queue, ))
        row = cur.fetchone()    
        if row is None:
            return None
        task = Task(id=row["id"], queue=row["queue"], priority=row["priority"], payload=row["payload"])
        cur.execute("""
            update queue set
            client = %s,
            locked_at = now(),
            release_at = now() + interval '%s minutes'
            where id = %s
         """, (client, release_minutes, task.id,))
        con.commit()
        cur.close()
        PostgresQueue.connection_pool.putconn(con)
        return task

    def complete(self, task_id: int) -> None:
        con = PostgresQueue.connection_pool.getconn()
        cur = con.cursor()
        cur.execute("""
            update queue
            set completed_at = now()
            where id = %s
        """, (task_id,))
        con.commit()
        cur.close()
        PostgresQueue.connection_pool.putconn(con)
"""
Example of message system using Postgres 
uses the idea outlined here: https://news.ycombinator.com/item?id=20020501
or here: https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/

Properties:

Multi consumer, multi producer
Both sides can be run multiple times

Durable messages
Consumers will read messages that have been produced before their start

Persistent messages
Messages are persisted

No duplicate processing
A message that consumer A has read will not be read by consumers B,C,...

Re-process messages when consumers fail
If a consumer fails before acknowledging the successful processing of a message,
the message will be re-processed by another worker.
=> at least once semantics

Pruning
Processed messages will be removed


How to run:
Requires running postres!

$ python -m db_streams producer

$ python -m db_streams consumer
"""

from typing import Optional
import dataclasses as _dc
import time as _time
import random
import sqlalchemy as _sa
import sqlalchemy.orm as _orm

import click


@click.group()
def main():
    pass


@_dc.dataclass
class Message:
    content: dict
    id: Optional[int] = None


mapper_registry = _orm.registry()
outbox_table = _sa.Table(
    "outbox",
    mapper_registry.metadata,
    _sa.Column("id", _sa.Integer, primary_key=True),
    _sa.Column("content", _sa.JSON()),
    _sa.Column("comp_aaa_processed", _sa.Boolean(), default=False),
    # TODO add every interested component as a column here
)
mapper_registry.map_imperatively(Message, outbox_table)


def init():
    # $ docker exec alasco-postgres bash -c 'psql -U selina -c "CREATE DATABASE db_streams"'
    engine = _sa.create_engine(
        "postgresql://selina:selina@localhost/db_streams",  # echo=True
    )
    Session = _orm.sessionmaker(bind=engine)
    mapper_registry.metadata.create_all(engine)
    return Session()


@main.command()
def producer():
    session = init()
    idx = 0
    while True:
        _time.sleep(random.random())
        idx += 1
        msg = Message(dict(attr=idx))
        session.add(msg)
        session.commit()
        print("." * next(session.execute("select count(*) from outbox"))[0])
        if idx % 5 == 0:
            session.execute(
                _sa.delete(
                    outbox_table,
                    whereclause=_sa.and_(
                        outbox_table.c.comp_aaa_processed
                        # TODO add interested components here as well
                    ),
                )
            )
            session.commit()


@main.command()
def consumer():
    session = init()
    while True:
        for msg in processable_messages(session, component_name="aaa"):
            try:
                _handle(session, msg)
            except Exception:
                session.rollback()
            else:
                session.commit()


def processable_messages(session, component_name):
    backoff = 0
    while True:
        # TODO DANGER!! use proper query building
        qry = f"""
        UPDATE outbox
        SET comp_{component_name}_processed = 't'
        WHERE id = (
          SELECT id
          FROM outbox
          WHERE NOT comp_{component_name}_processed
          ORDER BY id
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        RETURNING content;
        """
        res = list(session.execute(qry))
        if res:
            backoff = 0
            yield res[0][0]
        else:
            session.rollback()
            # linear backoff 1 to 4s
            backoff = min(backoff + 1, 4)
            _time.sleep(backoff)


def _handle(session, msg):
    fail = random.random() < 0.2
    print(msg, "!" if fail else "")
    if fail:
        raise RuntimeError
    # handling here happens in the session/transaction of the receiver
    _time.sleep(random.random() * 0.2)


if __name__ == "__main__":
    main()

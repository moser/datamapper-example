"""
Example of message system using redis streams.

Properties:

Multi consumer, multi producer
Both sides can be run multiple times

Durable messages
Consumers will read messages that have been produced before their start (if
the consumer has run once before and thus the consumer group in redis exists)
"Durable" does not mean "persistent" (here redis limits apply)

No duplicate processing
A message that consumer A has read will not be read by consumers B,C,...

Re-process messages when consumers fail
If a consumer fails before acknowledging the successful processing of a message,
the message will be re-processed by another worker after a time-out (2.5 sec here).
(the timeout needs to be high enough so that max processing time is always lower
than the timeout)
=> at least once semantics

Pruning
Processed messages will be removed


How to run:
Requires running redis on port 6379!

$ python -m redis_streams producer

$ python -m redis_streams consumer
"""

import os
import time
import random
import walrus
import click


@click.group()
def main():
    pass


STREAM_NAME = "xxx"


@main.command()
def producer():
    """
    Produces a messages
    """
    db = walrus.Database()
    stream = db.Stream(STREAM_NAME)
    idx = 0
    while True:
        idx += 1
        stream.add({"msg": idx})
        # Queue length monitoring -> send to datadog
        print("." * db.xlen(STREAM_NAME))
        time.sleep(0.4)
        if idx % 5 == 0:
            prune(db, STREAM_NAME)


def prune(db: walrus.Database, stream_name):
    # find the minimal id that is not processed (delivered or pending)
    ids = []
    for group in db.xinfo_groups(stream_name):
        ids.append(group["last-delivered-id"])
        pending = db.xpending(stream_name, group["name"])
        if pending["min"]:
            ids.append(pending["min"])
    if ids:
        min_id = min(ids)
        first_entry_id = db.xinfo_stream(stream_name)["first-entry"][0]
        # get the ids in the range [first_entry_id, min_id[
        ids = [
            id
            for id, _ in db.xrange(stream_name, min=first_entry_id, max=min_id)
            if id < min_id
        ]
        if ids:
            # prune unused ids
            db.xdel(stream_name, *ids)


@main.command()
def consumer():
    """
    Subscribes to the messages on the stream.
    Ack's messages in ~80% of the cases.
    Un-ack'd messages are reprocessed after 2.5s.
    """
    consumer_name = "cons-1"  # same everywhere -> no duplicate events
    # consumer name would be the component name, so that each component's
    # consumer processes would receive their own stream of events.
    db = walrus.Database()
    stream_names = [STREAM_NAME]
    cons = db.consumer_group("con-grp", stream_names, consumer=consumer_name)
    cons.create()
    while True:
        # get & process un-ack'd messages that have been idle for 2.5s
        for stream_key in stream_names:
            stream = getattr(cons, stream_key)
            # if walrus would support XAUTOCLAIM this would be simpler :-)
            pending = [pending["message_id"] for pending in stream.pending()]
            if pending:
                claimed = stream.claim(
                    *pending,
                    min_idle_time=2500,
                )
                for msg_id, msg in claimed:
                    if msg_id is not None:
                        print("re-process", msg)
                        _handle(stream, msg_id, msg)

        # get & process new messages
        for stream, messages in cons.read():
            stream = getattr(cons, stream.decode())
            for msg_id, msg in messages:
                _handle(stream, msg_id, msg)
        time.sleep(1)


def _handle(stream, msg_id, msg):
    fail = random.random() < 0.2
    print(msg, "!" if fail else "")
    if not fail:
        stream.ack(msg_id)


if __name__ == "__main__":
    main()

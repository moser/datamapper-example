from typing import List, Optional

import dataclasses as _dc

import sqlalchemy as _sa
import sqlalchemy.orm as _orm

import pytest as _pytest


def setup():
    @_dc.dataclass
    class DomainDog:
        id: int = None
        nom: str = None
        owner: "DomainUser" = None

    @_dc.dataclass
    class DomainUser:
        id: int = None
        name: str = None
        dogs: List[DomainDog] = _dc.field(default_factory=list)

    mapper_registry = _orm.registry()
    user_table = _sa.Table(
        "user",
        mapper_registry.metadata,
        _sa.Column("id", _sa.Integer, primary_key=True),
        _sa.Column("name", _sa.String(50)),
    )

    dog_table = _sa.Table(
        "dog",
        mapper_registry.metadata,
        _sa.Column("id", _sa.Integer, primary_key=True),
        _sa.Column("name", _sa.String(50)),
        _sa.Column("owner_id", _sa.Integer, _sa.ForeignKey(user_table.c.id)),
    )

    mapper_registry.map_imperatively(DomainUser, user_table)
    mapper_registry.map_imperatively(
        DomainDog,
        dog_table,
        properties={
            "owner": _orm.relationship(DomainUser, backref="dogs"),
            "nom": dog_table.c.name,
        },
    )
    return DomainUser, DomainDog, mapper_registry


def test_works():
    DomainUser, DomainDog, mapper_registry = setup()

    engine = _sa.create_engine("sqlite:///:memory:", echo=True)
    Session = _orm.sessionmaker(bind=engine)

    mapper_registry.metadata.create_all(engine)

    session = Session()
    usr = DomainUser(name="a")
    session.add(usr)
    session.add(DomainDog(nom="fifi", owner=usr))
    session.commit()

    print(session.query(DomainUser).first())


def setup_broken_constructor():
    @_dc.dataclass
    class DomainDog:
        id: int
        name: str
        owner: "DomainUser"

    @_dc.dataclass
    class DomainUser:
        id: int
        name: str
        dogs: List[DomainDog] = _dc.field(default_factory=list)

    mapper_registry = _orm.registry()
    user_table = _sa.Table(
        "user",
        mapper_registry.metadata,
        _sa.Column("id", _sa.Integer, primary_key=True),
        _sa.Column("name", _sa.String(50)),
    )

    dog_table = _sa.Table(
        "dog",
        mapper_registry.metadata,
        _sa.Column("id", _sa.Integer, primary_key=True),
        _sa.Column("name", _sa.String(50)),
        _sa.Column("owner_id", _sa.Integer, _sa.ForeignKey(user_table.c.id)),
    )

    mapper_registry.map_imperatively(DomainUser, user_table)
    mapper_registry.map_imperatively(
        DomainDog,
        dog_table,
        properties={"owner": _orm.relationship(DomainUser, backref="dogs")},
    )
    return DomainUser, DomainDog, mapper_registry


def test_non_relaxed_constructor():
    DomainUser, DomainDog, mapper_registry = setup_broken_constructor()

    engine = _sa.create_engine("sqlite:///:memory:", echo=True)
    Session = _orm.sessionmaker(bind=engine)

    mapper_registry.metadata.create_all(engine)

    session = Session()
    # here we have to give the full args!!
    usr = DomainUser(id=None, name="a")
    session.add(usr)
    # here we have to give the full args!!
    session.add(DomainDog(id=None, name="fifi", owner=usr))
    session.commit()

    print(session.query(DomainUser).first())

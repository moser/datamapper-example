from typing import List, Optional

import dataclasses as _dc

import sqlalchemy as _sa
import sqlalchemy.orm as _orm

import pytest as _pytest


def setup_domain():
    @_dc.dataclass
    class DomainDog:
        id: int = None
        name: str = None
        owner: "DomainUser" = None

    @_dc.dataclass
    class DomainUser:
        id: int = None
        name: str = None
        dogs: List[DomainDog] = _dc.field(default_factory=list)

    return DomainDog, DomainUser


def setup_mapping(DomainDog, DomainUser):
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


def scenario_simple(DomainDog, DomainUser):
    usr = DomainUser(name="a")
    dog = DomainDog(name="a")
    usr.dogs.append(dog)
    # Fails after mapping, because `owner` is a sqla InstrumentedList now and
    # syncs the change to its backref
    assert dog.owner is None


def scenario_backwards(DomainDog, DomainUser):
    dog = DomainDog(name="a", owner=DomainUser())
    # Same effect but in different direction
    assert not dog.owner.dogs


@_pytest.mark.parametrize("scenario", [scenario_simple, scenario_backwards])
def test_before_and_after(scenario):
    """
    Runs scenarios that succeed before the mapping but fail afterwards
    """
    cls = setup_domain()
    scenario(*cls)
    setup_mapping(*cls)
    with _pytest.raises(AssertionError):
        scenario(*cls)

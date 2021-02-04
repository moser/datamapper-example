from typing import List, Optional, Tuple, Type

import dataclasses as _dc
import inflection as _inflection

import sqlalchemy as _sa
import sqlalchemy.orm as _orm

import pytest as _pytest


def delegate_to(attr_name: str):
    def _inner(cls):
        def fn(self, name):
            return getattr(getattr(self, attr_name), name)

        cls.__getattr__ = fn
        return cls

    return _inner


@delegate_to("session")
class TwinMapper:
    mapping: List[Tuple[Type, Type]]

    def __init__(self, session):
        self.session = session

        self._domain_to_orm = {}
        self._orm_to_domain = {}
        self._domain_to_orm_classes = {
            domain_cls: orm_cls for domain_cls, orm_cls in self.mapping
        }
        self._orm_to_domain_classes = {
            orm_cls: domain_cls for domain_cls, orm_cls in self.mapping
        }

    def add(self, domain_obj):
        orm_cls = self.get_orm_cls(domain_obj)
        orm_obj = orm_cls()
        self._add_twin(domain_obj, orm_obj)
        self._update_orm(domain_obj, orm_obj)
        self.session.add(orm_obj)

    def query(self, domain_cls):
        orm_cls = self.get_orm_cls(domain_cls)
        return self.session.query(orm_cls)

    def to_domain(self, orm_obj):
        if orm_obj is None:
            return None

        if orm_obj not in self._orm_to_domain:
            domain_cls = self.get_domain_cls(orm_obj)
            domain_obj = domain_cls()
            self._orm_to_domain[orm_obj] = domain_obj
            self._update_domain(orm_obj, domain_obj)
        return self._orm_to_domain[orm_obj]

    def to_orm(self, domain_obj):
        if domain_obj is None:
            return None

        if domain_obj not in self._domain_to_orm:
            orm_cls = self.get_orm_cls(domain_obj)
            orm_obj = orm_cls()
            self._domain_to_orm[domain_obj] = orm_obj
            self._update_orm(domain_obj, orm_obj)
        return self._domain_to_orm[domain_obj]

    def get_orm_cls(self, obj_or_class):
        if isinstance(obj_or_class, type):
            return self._domain_to_orm_classes[obj_or_class]
        return self._domain_to_orm_classes[obj_or_class.__class__]

    def get_domain_cls(self, obj_or_class):
        if isinstance(obj_or_class, type):
            return self._orm_to_domain_classes[obj_or_class]
        return self._orm_to_domain_classes[obj_or_class.__class__]

    def _add_twin(self, domain_obj, orm_obj):
        self._domain_to_orm[domain_obj] = orm_obj
        self._orm_to_domain[orm_obj] = domain_obj

    def _update_orm(self, domain_obj, orm_obj):
        domain_cls_name = _inflection.underscore(domain_obj.__class__.__name__)

        mapper = getattr(self, f"map_{domain_cls_name}")
        mapper(domain_obj, orm_obj)

    def _update_domain(self, orm_obj, domain_obj):
        orm_cls_name = _inflection.underscore(orm_obj.__class__.__name__)

        mapper = getattr(self, f"map_{orm_cls_name}")
        mapper(orm_obj, domain_obj)


def setup():
    class DomainDog:
        id: int
        name: str
        owner: "DomainUser"

        def __init__(self, id=None, name=None, owner=None):
            self.id = id
            self.name = name
            self.owner = owner

    class DomainUser:
        id: int
        name: str
        dogs: List[DomainDog]

        def __init__(self, id=None, name=None, dogs=None):
            self.id = id
            self.name = name
            self.dogs = dogs or []

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

    class OrmDog:
        pass

    class OrmUser:
        pass

    mapper_registry.map_imperatively(OrmUser, user_table)
    mapper_registry.map_imperatively(
        OrmDog,
        dog_table,
        properties={"owner": _orm.relationship(OrmUser, backref="dogs")},
    )

    class ConcreteTwinMapper(TwinMapper):
        mapping = [
            (DomainUser, OrmUser),
            (DomainDog, OrmDog),
        ]

        def map_domain_user(self, domain_user: DomainUser, orm_user: OrmUser):
            orm_user.name = domain_user.name
            orm_user.dogs = domain_user.dogs

        def map_domain_dog(self, domain_dog: DomainDog, orm_dog: OrmDog):
            orm_dog.name = domain_dog.name
            orm_dog.owner = self.to_orm(domain_dog.owner)

        def map_orm_user(self, orm_user: OrmUser, domain_user: DomainUser):
            domain_user.name = orm_user.name
            domain_user.dogs = list(map(self.to_domain, orm_user.dogs))

        def map_orm_dog(self, orm_dog: OrmDog, domain_dog: DomainDog):
            domain_dog.name = orm_dog.name
            domain_dog.owner = self.to_domain(orm_dog.owner)

    return DomainUser, DomainDog, ConcreteTwinMapper, mapper_registry


def test_works():
    DomainUser, DomainDog, Mapper, mapper_registry = setup()

    engine = _sa.create_engine("sqlite:///:memory:")
    # engine = _sa.create_engine("sqlite:///:memory:", echo=True)
    Session = _orm.sessionmaker(bind=engine)

    mapper_registry.metadata.create_all(engine)

    session = Mapper(Session())
    usr = DomainUser(name="a")
    # session.add(usr)
    session.add(DomainDog(name="fifi", owner=usr))
    session.add(DomainDog(name="fifi"))
    session.commit()

    session = Mapper(Session())
    user = session.query(DomainUser).first()
    print(user)
    print(session.to_domain(user))

    dog = session.query(DomainDog).first()
    print(dog)
    print(session.to_domain(dog))

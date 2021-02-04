from typing import Optional
from unittest import mock
import pydantic as _pydantic
import pytest as _pytest


class D(_pydantic.BaseModel):
    pass


class C(_pydantic.BaseModel):
    something: str
    x: Optional[D]


def test_pass_mock():
    with _pytest.raises(_pydantic.ValidationError):
        C(something=mock.MagicMock())

    with _pytest.raises(_pydantic.ValidationError):
        # not even this :-(
        C(something=mock.create_autospec(D))


class WithValidator:
    def __init__(self, amount, currency):
        self.amount = amount
        self.currency = currency

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, values):
        import decimal

        assert "amount" in values
        assert "currency" in values
        assert values["currency"] in ["EUR", "USD"]
        amount = _pydantic.parse_obj_as(decimal.Decimal, values["amount"])
        return cls(amount, values["currency"])

    def __repr__(self):
        return f"<WithValidator amount={self.amount} currency={self.currency}>"


def test_validator():
    class X(_pydantic.BaseModel):
        le_attr: WithValidator

    print(X(le_attr=dict(amount=1.1, currency="EUR")))


class WithSchema(WithValidator):
    @classmethod
    def __modify_schema__(cls, field_schema):
        # __modify_schema__ should mutate the dict it receives in place,
        # the returned value will be ignored
        print("field", field_schema)
        field_schema.update(
            type="object",
            properties=dict(
                amount=dict(type="str", example="123.45"),
                currency=dict(type="str", example="EUR"),
            ),
        )


def test_schema():
    class Y(_pydantic.BaseModel):
        my_arg: WithSchema

    print(Y.schema())


def test_late():
    class NoValidator:
        def __init__(self, amount, currency):
            self.amount = amount
            self.currency = currency

    def validate(values):
        import decimal

        assert "amount" in values
        assert "currency" in values
        assert values["currency"] in ["EUR", "USD"]
        amount = _pydantic.parse_obj_as(decimal.Decimal, values["amount"])
        return NoValidator(amount, values["currency"])

    def get_validators():
        yield validate

    def modify_schema(field_schema):
        field_schema.update(
            type="object",
            properties=dict(
                amount=dict(type="str", example="123.45"),
                currency=dict(type="str", example="EUR"),
            ),
        )

    # alter the class :-)
    NoValidator.__get_validators__ = get_validators
    NoValidator.__modify_schema__ = modify_schema

    class X(_pydantic.BaseModel):
        my_arg: NoValidator

    print(X(my_arg=dict(amount=1.1, currency="EUR")))
    print(X.schema())

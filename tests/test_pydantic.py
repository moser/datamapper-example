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

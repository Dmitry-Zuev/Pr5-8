import pytest
from rabbitmq.exercise import get_delay, VARIANTS

def test_variant_table():
    assert len(VARIANTS) == 4
    assert VARIANTS[1]["exchange"] == "topic"
    assert VARIANTS[2]["exchange"] == "direct"
    assert VARIANTS[3]["exchange"] == "fanout"

@pytest.mark.parametrize("variant,message,expected", [(1, "a..b", 2), (2, "abc", 3), (4, "5", 5)])
def test_delay(variant, message, expected):
    assert get_delay(variant, message) == expected

def test_four_requires_integer():
    with pytest.raises(ValueError):
        get_delay(4, "word")

def test_three_delay_within_1_to_5():
    assert 1 <= get_delay(3, "anything") <= 5

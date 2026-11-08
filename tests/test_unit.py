"""Unit tests (no databases and no external services)."""
import pytest
from pydantic import ValidationError
from services.users.main import hash_password, verify_password, Register
from services.trainings.main import TrainingCreate
from services.reviews.main import Create as ReviewCreate
from services.payments.main import Create as PaymentCreate
from services.subscriptions.main import Change as SubscriptionChange
from services.tours.main import Create as TourCreate


def test_password_hash_does_not_store_plaintext():
    encoded = hash_password("secret-password")
    assert "secret-password" not in encoded
    assert verify_password("secret-password", encoded)
    assert not verify_password("not-it", encoded)


def test_register_requires_correct_email():
    with pytest.raises(ValidationError):
        Register(email="not-email", name="Клиент", password="password123")


def test_trainings_require_positive_capacity():
    with pytest.raises(ValidationError):
        TrainingCreate(title="Тренировка", starts_at="2026-12-01T10:00:00", capacity=0)


def test_review_requires_nonempty_text():
    with pytest.raises(ValidationError):
        ReviewCreate(training_id="a", user_id="b", text="")


def test_payment_requires_positive_amount():
    with pytest.raises(ValidationError):
        PaymentCreate(user_id="u", amount="0")


def test_subscription_action_parsing():
    change = SubscriptionChange(action="freeze")
    assert change.action == "freeze"
    assert change.payment_id is None


def test_tour_requires_applicant_name():
    with pytest.raises(ValidationError):
        TourCreate(applicant_name="", preferred_at="2026-12-01T10:00:00")

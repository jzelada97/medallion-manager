"""Tests for config, logging helpers and the consumer decode logic."""

from __future__ import annotations

from hr_etl.config import Settings
from hr_etl.consumer.kafka_consumer import KafkaMessageConsumer
from hr_etl.logging_conf import configure_logging, get_logger, mask_secret


def test_settings_postgres_dsn():
    s = Settings(
        postgres_user="u", postgres_password="p", postgres_host="h",
        postgres_port=5432, postgres_db="db",
    )
    assert s.postgres_dsn == "postgresql+psycopg2://u:p@h:5432/db"


def test_mask_secret():
    assert mask_secret("ES9121000418450200051332").endswith("1332")
    assert mask_secret("ES9121000418450200051332").startswith("*")
    assert mask_secret(None) == ""
    assert mask_secret("ab") == "**"


def test_logging_configures_once():
    configure_logging("DEBUG")
    configure_logging("INFO")  # second call is a no-op, must not raise
    assert get_logger("x") is not None


def test_consumer_decode_valid():
    assert KafkaMessageConsumer.decode(b'{"a": 1}') == {"a": 1}
    assert KafkaMessageConsumer.decode('{"b": 2}') == {"b": 2}


def test_consumer_decode_invalid():
    assert KafkaMessageConsumer.decode(None) is None
    assert KafkaMessageConsumer.decode(b"not json") is None
    assert KafkaMessageConsumer.decode(b"[1,2,3]") is None  # not a dict


def test_consumer_consume_skips_none_and_errors():
    """poll() returning None and messages with errors are skipped gracefully."""

    class ErrMsg:
        def error(self):
            return "boom"

        def value(self):
            return None

    class FakeConsumer:
        def __init__(self):
            # None (timeout), an error message, then a valid one
            self._seq = [None, ErrMsg(), _ValidMsg()]
            self.closed = False

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            return self._seq.pop(0) if self._seq else None

        def commit(self, msg, asynchronous=False):
            pass

        def close(self):
            self.closed = True

    settings = Settings()
    consumer = KafkaMessageConsumer(settings, consumer=FakeConsumer())
    out = list(consumer.consume(max_messages=1))
    assert out == [{"ok": 1}]


class _ValidMsg:
    def error(self):
        return None

    def value(self):
        return b'{"ok": 1}'


def test_consumer_consume_with_fake(monkeypatch):
    """Drive consume() with a fake confluent-style consumer."""

    class FakeMsg:
        def __init__(self, value):
            self._value = value

        def error(self):
            return None

        def value(self):
            return self._value

    class FakeConsumer:
        def __init__(self):
            self._msgs = [FakeMsg(b'{"Name":"Ana","Lastname":"Gil","Sex":"F","Telfnumber":"1","Passport":"X1","E-Mail":"a@b.c"}')]
            self.committed = 0
            self.closed = False

        def subscribe(self, topics):
            self.topics = topics

        def poll(self, timeout):
            return self._msgs.pop(0) if self._msgs else None

        def commit(self, msg, asynchronous=False):
            self.committed += 1

        def close(self):
            self.closed = True

    settings = Settings()
    fake = FakeConsumer()
    consumer = KafkaMessageConsumer(settings, consumer=fake)
    out = list(consumer.consume(max_messages=1))
    assert out == [
        {"Name": "Ana", "Lastname": "Gil", "Sex": "F", "Telfnumber": "1", "Passport": "X1", "E-Mail": "a@b.c"}
    ]
    assert fake.closed is True


def test_pii_masking_filter_passport(capfd):
    """The PII filter masks passport values in log output."""
    import logging
    import sys

    from hr_etl.logging_conf import PIIMaskingFilter

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(PIIMaskingFilter())
    handler.setFormatter(logging.Formatter("%(message)s"))

    test_logger = logging.getLogger("test.pii.passport")
    test_logger.handlers = [handler]
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    test_logger.info("fragment key=passport=%s", "H85111106")
    captured = capfd.readouterr()
    assert "H85111106" not in captured.out
    assert "***masked***" in captured.out


def test_pii_masking_filter_iban(capfd):
    """The PII filter masks IBAN values in log output."""
    import logging
    import sys

    from hr_etl.logging_conf import PIIMaskingFilter

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(PIIMaskingFilter())
    handler.setFormatter(logging.Formatter("%(message)s"))

    test_logger = logging.getLogger("test.pii.iban")
    test_logger.handlers = [handler]
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    test_logger.info("person iban=ES9121000418450200051332")
    captured = capfd.readouterr()
    assert "ES9121000418450200051332" not in captured.out
    assert "ES91***masked***" in captured.out


def test_pii_masking_filter_email(capfd):
    """The PII filter masks email addresses in log output."""
    import logging
    import sys

    from hr_etl.logging_conf import PIIMaskingFilter

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(PIIMaskingFilter())
    handler.setFormatter(logging.Formatter("%(message)s"))

    test_logger = logging.getLogger("test.pii.email")
    test_logger.handlers = [handler]
    test_logger.setLevel(logging.DEBUG)
    test_logger.propagate = False

    test_logger.info("contact email=john.doe@example.com found")
    captured = capfd.readouterr()
    assert "john.doe" not in captured.out
    assert "***@masked***" in captured.out

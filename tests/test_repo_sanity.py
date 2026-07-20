from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_spark_no_truncate_cascade():
    text = (ROOT / "spark" / "spark.py").read_text(encoding="utf-8")
    assert "TRUNCATE TABLE public.dim_date CASCADE" not in text
    assert "monotonically_increasing_id()" not in text


def test_kafka_manual_commit_enabled():
    producer_text = (ROOT / "kafka" / "producer_app.py").read_text(encoding="utf-8")
    consumer_text = (ROOT / "kafka" / "consumer_app.py").read_text(encoding="utf-8")
    assert "enable_auto_commit=True" not in producer_text
    assert "enable_auto_commit=True" not in consumer_text
    assert "consumer.commit(offsets=offsets)" in consumer_text
    assert "consumer.commit(offsets=offsets)" in producer_text


def test_compose_no_external_network_and_has_healthchecks():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "external: true" not in text
    assert "healthcheck:" in text

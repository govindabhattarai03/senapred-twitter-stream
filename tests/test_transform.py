from datetime import timezone

from pipeline.transform import enrich, find_keywords, find_location, normalize, parse_created_at


def test_normalize_strips_accents_and_case():
    assert normalize("EVACUACIÓN en Valparaíso") == "evacuacion en valparaiso"


def test_find_keywords_ignores_accents_and_plurals():
    assert find_keywords("Evacuacion y SISMOS en la costa") == ["evacuación", "sismo"]


def test_find_keywords_none():
    assert find_keywords("Hoy hace buen tiempo") == []


def test_location_prefers_geo_place_name():
    assert find_location("sismo en Santiago", {"place_name": "Temuco"}) == "Temuco"


def test_location_falls_back_to_text():
    assert find_location("Incendio cerca de puerto montt") == "Puerto Montt"
    assert find_location("Incendio forestal") is None


def test_parse_x_api_timestamp_is_utc():
    dt = parse_created_at("2025-11-18T00:41:36.000Z")
    assert dt.tzinfo == timezone.utc and dt.hour == 0


def test_enrich_builds_clean_document():
    doc = enrich(
        {"id": 123, "text": "Alerta de tsunami en Iquique", "author_id": 9,
         "created_at": "2025-11-18T00:41:36Z"},
        source="x_api",
    )
    assert doc["tweet_id"] == "123"
    assert doc["keywords"] == ["alerta", "tsunami"]
    assert doc["location"] == "Iquique"
    assert doc["source"] == "x_api"

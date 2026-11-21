import json
import pytest
from livestock_weather.settings import load_settings


def test_demo_settings_valid():
    cfg = load_settings("config/demo.json")
    assert cfg["weights"]["heat"] == 0.4


def test_weights_need_to_sum_to_one(tmp_path):
    cfg = load_settings("config/demo.json")
    cfg["weights"]["heat"] = 0.99
    file = tmp_path / "bad.json"
    file.write_text(json.dumps(cfg))
    with pytest.raises(ValueError):
        load_settings(file)

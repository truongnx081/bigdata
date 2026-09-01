from services.common.traffic_rules import congestion_level, risk_score


def test_free_flow_is_smooth() -> None:
    assert congestion_level(55, 24) == "smooth"


def test_low_speed_alone_is_moderate_at_medium_density() -> None:
    assert congestion_level(12, 47) == "moderate"


def test_high_density_and_low_speed_is_critical() -> None:
    assert congestion_level(12, 88) == "critical"


def test_high_density_with_moving_traffic_is_moderate() -> None:
    assert congestion_level(41, 88) == "moderate"


def test_risk_score_is_bounded() -> None:
    assert risk_score(120, 0, 0) == 0
    assert risk_score(0, 100, 2) == 100


def test_anomaly_increases_risk() -> None:
    assert risk_score(38, 48, 0.8) > risk_score(38, 48, 0.1)

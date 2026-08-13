import json

from eovrt_distribution.sources import DirectSource, JsonlReplaySource


def test_direct_source_yields_all(make_alert):
    alerts = [make_alert(), make_alert()]
    src = DirectSource(alerts)
    got = list(src)
    assert [s.alert["alert_id"] for s in got] == [a["alert_id"] for a in alerts]
    assert all(s.ts_publish_ms is None for s in got)


def test_jsonl_replay_reads_lines_and_counts_malformed(tmp_path, make_alert):
    path = tmp_path / "alerts.jsonl"
    good1, good2 = make_alert(), make_alert()
    path.write_text(
        json.dumps(good1) + "\n" + "{esto no es json}\n" + "\n" + json.dumps(good2) + "\n"
    )
    src = JsonlReplaySource(path)
    got = list(src)
    assert [s.alert["alert_id"] for s in got] == [good1["alert_id"], good2["alert_id"]]
    assert src.stats == {"read": 2, "skipped_malformed": 1}

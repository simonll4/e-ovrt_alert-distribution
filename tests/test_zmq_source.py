import json
import threading
import time

import msgpack
import pytest
import zmq

from eovrt_distribution.transport.envelope import decode_envelope
from eovrt_distribution.transport.zmq_source import ZmqSource


def _envelope_frames(topic: str, seq: int, payload: dict, ts: float = 1750000000000.0):
    env = {
        "schema_version": "bus.envelope.v1",
        "topic": topic,
        "key": payload.get("source_id", ""),
        "seq": seq,
        "ts_publish_ms": ts,
        "payload": json.dumps(payload, ensure_ascii=True).encode(),
    }
    return [topic.encode(), msgpack.packb(env)]


def test_decode_envelope_roundtrip(make_alert):
    alert = make_alert()
    frames = _envelope_frames("control.alert.v1.cr-test", 0, alert)
    decoded = decode_envelope(frames)
    assert decoded["seq"] == 0
    assert decoded["ts_publish_ms"] == 1750000000000.0
    assert decoded["payload"]["alert_id"] == alert["alert_id"]


def test_decode_envelope_rejects_bad_schema():
    frames = [b"t", msgpack.packb({"schema_version": "otra.cosa"})]
    with pytest.raises(ValueError):
        decode_envelope(frames)


def test_decode_envelope_rejects_wire_and_inner_topic_mismatch(make_alert):
    frames = _envelope_frames("control.alert.v1.cr-test", 0, make_alert())
    frames[0] = b"run.lifecycle.v1.other"
    with pytest.raises(ValueError, match="topic"):
        decode_envelope(frames)


def test_decode_envelope_rejects_non_finite_publish_timestamp(make_alert):
    frames = _envelope_frames("control.alert.v1.cr-test", 0, make_alert(), ts=float("nan"))
    with pytest.raises(ValueError, match="ts_publish_ms"):
        decode_envelope(frames)


@pytest.fixture
def publisher():
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.XPUB)
    port = sock.bind_to_random_port("tcp://127.0.0.1")
    yield sock, f"tcp://127.0.0.1:{port}"
    sock.close(0)


def _wait_subscriptions(sock, expected: int = 2) -> None:
    """El ZmqSource emite DOS suscripciones (alertas + lifecycle); publicar
    antes de que el XPUB procese ambas filtraría mensajes y colgaría el test."""
    for _ in range(expected):
        sock.recv()


def _run_source(source):
    got = []

    def consume():
        got.extend(source)

    thread = threading.Thread(target=consume)
    thread.start()
    return got, thread


def test_live_stream_until_run_finished(publisher, make_alert):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint)
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    a1, a2 = make_alert(), make_alert()
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, a1))
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 1, a2))
    sock.send_multipart(_envelope_frames("run.lifecycle.v1.cr-test", 2, {"event": "run_finished"}))
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert [s.alert["alert_id"] for s in got] == [a1["alert_id"], a2["alert_id"]]
    assert got[0].ts_publish_ms == 1750000000000.0
    assert source.stats["bus_dropped_events"] == 0


def test_seq_gap_counted(publisher, make_alert):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint)
    _got, thread = _run_source(source)
    _wait_subscriptions(sock)
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, make_alert()))
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 3, make_alert()))
    sock.send_multipart(_envelope_frames("run.lifecycle.v1.cr-test", 4, {"event": "run_finished"}))
    thread.join(timeout=5)
    assert source.stats["bus_dropped_events"] == 2


def test_initial_seq_gap_is_counted(publisher, make_alert):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint)
    _got, thread = _run_source(source)
    _wait_subscriptions(sock)
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 3, make_alert()))
    sock.send_multipart(_envelope_frames("run.lifecycle.v1.cr-test", 4, {"event": "run_finished"}))
    thread.join(timeout=5)
    assert source.stats["bus_dropped_events"] == 3


def test_backfill_then_stream_dedupes_by_alert_id(publisher, tmp_path, make_alert):
    sock, endpoint = publisher
    early, late = make_alert(), make_alert()
    backfill = tmp_path / "alerts.jsonl"
    backfill.write_text(json.dumps(early) + "\n")
    source = ZmqSource(endpoint=endpoint, backfill_path=backfill)
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, early))  # dup
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 1, late))
    sock.send_multipart(_envelope_frames("run.lifecycle.v1.cr-test", 2, {"event": "run_finished"}))
    thread.join(timeout=5)
    assert [s.alert["alert_id"] for s in got] == [early["alert_id"], late["alert_id"]]
    assert source.stats["duplicates_from_backfill"] == 1
    assert source.stats["backfill_read"] == 1


def test_request_stop_terminates_without_run_finished(publisher):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint, recv_timeout_ms=50)
    _got, thread = _run_source(source)
    _wait_subscriptions(sock)
    time.sleep(0.2)
    source.request_stop()
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_lifecycle_for_other_run_does_not_stop_target_run(publisher, make_alert):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint, control_run_id="cr-target")
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    sock.send_multipart(_envelope_frames("run.lifecycle.v1.cr-other", 0, {"event": "run_finished"}))
    alert = make_alert(control_run_id="cr-target")
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-target", 1, alert))
    sock.send_multipart(
        _envelope_frames("run.lifecycle.v1.cr-target", 2, {"event": "run_finished"})
    )
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert [item.alert["alert_id"] for item in got] == [alert["alert_id"]]

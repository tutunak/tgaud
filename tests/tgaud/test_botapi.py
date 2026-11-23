import logging
import threading
import time
import traceback

import httpx
import pytest
from botapi_fake import TOKEN, FakeBotApi, FakeBotServer

from tgaud import metrics
from tgaud.botapi import ApiError, BotApi


def count(method, outcome):
    return metrics.REGISTRY.get_sample_value("tgaud_api_requests_total", {"method": method, "outcome": outcome}) or 0


def test_ok_and_typed_helpers():
    fake = FakeBotApi()
    api = fake.api()
    before = count("getMe", "ok")
    assert api.get_me()["id"] == 5000
    assert count("getMe", "ok") == before + 1
    fake.ok("getChatMemberCount", 42)
    assert api.get_chat_member_count(-100) == 42
    fake.updates([{"update_id": 1}])
    assert api.get_updates(10) == [{"update_id": 1}]
    call = fake.calls_of("getUpdates")[-1]
    assert call.params == {"offset": 10, "timeout": 8, "limit": 100,
                           "allowed_updates": ["chat_member", "my_chat_member", "chat_join_request", "message",
                                               "channel_post"]}
    fake.respond("getChat", lambda p: {"id": p["chat_id"], "type": "channel", "title": "t"})
    assert api.get_chat(-7)["id"] == -7
    fake.ok("getChatMember", {"status": "left"})
    assert api.get_chat_member(-7, 9)["status"] == "left"
    fake.ok("createChatInviteLink", {"invite_link": "https://t.me/+x", "name": "G1"})
    assert api.create_chat_invite_link(-7, name="G1")["name"] == "G1"
    assert fake.calls_of("createChatInviteLink")[-1].params == {"chat_id": -7, "name": "G1"}
    seconds = metrics.REGISTRY.get_sample_value("tgaud_api_request_seconds_count", {"method": "getMe"})
    assert seconds and seconds >= 1


@pytest.mark.parametrize("code,outcome", [(429, "429"), (403, "403"), (400, "400"), (500, "5xx"), (502, "5xx"),
                                          (401, "other"), (409, "other"), (404, "other")])
def test_error_outcome_classes(code, outcome):
    fake = FakeBotApi()
    fake.error("getChatMember", code, f"error {code}", retry_after=7 if code == 429 else None)
    before = count("getChatMember", outcome)
    with pytest.raises(ApiError) as e:
        fake.api().get_chat_member(-1, 2)
    assert e.value.outcome == outcome and e.value.error_code == code
    assert e.value.retry_after == (7.0 if code == 429 else None)
    assert count("getChatMember", outcome) == before + 1


def test_timeout_transport_and_bad_json():
    fake = FakeBotApi()
    fake.raise_timeout("getUpdates")
    with pytest.raises(ApiError) as e:
        fake.api().get_updates(0)
    assert e.value.outcome == "timeout"
    fake.raise_transport_error("getMe")
    with pytest.raises(ApiError) as e:
        fake.api().get_me()
    assert e.value.outcome == "other"
    fake.raw("getMe", 200, b"<html>not json</html>")
    with pytest.raises(ApiError) as e:
        fake.api().get_me()
    assert e.value.outcome == "other"
    fake.raw("getMe", 502, b"Bad Gateway")
    with pytest.raises(ApiError) as e:
        fake.api().get_me()
    assert e.value.outcome == "5xx"


def _chain(exc: BaseException) -> list[BaseException]:
    out, seen = [], set()
    while exc is not None and id(exc) not in seen:
        out.append(exc)
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return out


def _assert_no_token(err: ApiError):
    assert err.__cause__ is None and err.__context__ is None
    text = " ".join([str(err), repr(err), err.description, "".join(traceback.format_exception(err))])
    for e in _chain(err):
        text += repr(e) + str(e) + repr(vars(e))
    assert TOKEN not in text
    assert TOKEN.split(":")[1] not in text


def test_token_never_in_errors_or_their_chain(caplog):
    caplog.set_level(logging.DEBUG)
    fake = FakeBotApi()
    fake.error("getChat", 400, f"Bad Request: bot{TOKEN} and {TOKEN} echoed")
    with pytest.raises(ApiError) as e:
        fake.api().get_chat(1)
    _assert_no_token(e.value)
    fake.raise_transport_error("getChat")  # the httpx error message carries the token-bearing URL
    with pytest.raises(ApiError) as e:
        fake.api().get_chat(1)
    assert "***" in e.value.description
    _assert_no_token(e.value)
    # a real transport failure: nothing listens on port 1
    api = BotApi(TOKEN, "http://127.0.0.1:1", connect_timeout=1)
    with pytest.raises(ApiError) as e:
        api.get_me()
    _assert_no_token(e.value)
    assert TOKEN not in repr(api)
    from tgaud import config

    formatter = config.RedactingFormatter([TOKEN])
    for record in caplog.records:
        assert TOKEN not in formatter.format(record)


def test_quiet_8s_long_poll_succeeds():
    with FakeBotServer() as server:
        server.hold("getUpdates", seconds=8.0, then=None)
        api = server.api()
        started = time.monotonic()
        assert api.get_updates(1, timeout=8) == []
        assert 7.5 < time.monotonic() - started < 12


def test_trickling_response_times_out_per_phase_read_timeout():
    with FakeBotServer() as server:
        server.trickle_headers("getMe", interval=2.0)
        api = server.api(read_timeout=1.0)
        started = time.monotonic()
        with pytest.raises(ApiError) as e:
            api.get_me()
        assert e.value.outcome == "timeout"
        assert time.monotonic() - started < 5


def test_close_from_another_thread_is_safe():
    with FakeBotServer() as server:
        server.hold("getUpdates", seconds=None)
        api = server.api()
        out = {}

        def poll():
            try:
                out["r"] = api.get_updates(1, timeout=1)
            except ApiError as e:
                out["e"] = e

        t = threading.Thread(target=poll, daemon=True)
        t.start()
        time.sleep(0.3)
        api.close()
        t.join(15)
        assert not t.is_alive()


def test_method_names_are_checked():
    with pytest.raises(ValueError):
        FakeBotApi().api().call("getMe/../x")
    with pytest.raises(ValueError):
        BotApi("", "https://x")
    assert isinstance(FakeBotApi().transport, httpx.MockTransport)

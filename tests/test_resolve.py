import pytest

from tgstats.analysis import AnalysisError, resolve_chat
from tgstats.db import ChatRow

CHATS = [
    ChatRow(-1001, "channel", "News", "newsline"),
    ChatRow(-1002, "supergroup", "Chat", "NewsChat"),
    ChatRow(-77, "group", "Family", None),
    ChatRow(-1003, "group", "Twin", None),
    ChatRow(-1004, "group", "Twin", None),
    ChatRow(-1005, "group", "2024", None),
]


def test_by_numeric_id():
    assert resolve_chat("-1001", CHATS).id == -1001
    assert resolve_chat("-77", CHATS).title == "Family"


def test_by_bare_numeric_id():
    assert resolve_chat("77", CHATS).title == "Family"
    assert resolve_chat("1001", [ChatRow(-1000000001001, "channel", "Big", None)]).title == "Big"


def test_by_username_case_insensitive_with_at():
    assert resolve_chat("@newsline", CHATS).id == -1001
    assert resolve_chat("@NEWSCHAT", CHATS).id == -1002


def test_by_exact_title():
    assert resolve_chat("Family", CHATS).id == -77
    with pytest.raises(AnalysisError):
        resolve_chat("family", CHATS)  # exact, not fuzzy


def test_numeric_title_falls_back_to_title():
    assert resolve_chat("2024", CHATS).id == -1005


def test_ambiguous_lists_candidates():
    with pytest.raises(AnalysisError) as err:
        resolve_chat("Twin", CHATS)
    msg = str(err.value)
    assert "ambiguous" in msg and "-1003" in msg and "-1004" in msg


def test_unknown_lists_known_chats():
    with pytest.raises(AnalysisError) as err:
        resolve_chat("@nobody", CHATS)
    assert "unknown" in str(err.value) and "Family" in str(err.value)
    with pytest.raises(AnalysisError, match="unknown"):
        resolve_chat("-999", CHATS)

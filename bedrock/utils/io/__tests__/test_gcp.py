import typing as ta
from unittest.mock import MagicMock

import pandas as pd
import pytest
import tenacity
from googleapiclient.errors import HttpError

from bedrock.utils.io import gcp


def test_create_spreadsheet_in_folder_calls_drive_create_with_expected_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = MagicMock()
    mock_client.files.return_value.create.return_value.execute.return_value = {
        "id": "newSheetId123"
    }

    monkeypatch.setattr(gcp, "__drive_client", lambda: mock_client)

    result = gcp.create_spreadsheet_in_folder(
        title="my_run_2026_04_28", folder_id="folder_abc"
    )

    assert result == "newSheetId123"
    create_call = mock_client.files.return_value.create.call_args_list[-1]
    body = create_call.kwargs["body"]
    assert body["name"] == "my_run_2026_04_28"
    assert body["mimeType"] == "application/vnd.google-apps.spreadsheet"
    assert body["parents"] == ["folder_abc"]
    assert create_call.kwargs["fields"] == "id"
    assert create_call.kwargs["supportsAllDrives"] is True


def _http_error(status: int) -> HttpError:
    return HttpError(MagicMock(status=status, reason="boom"), b"{}")


@pytest.fixture
def no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the retry-path tests fast by zeroing the tenacity backoff."""
    for fn in (
        gcp.read_sheet_tab,
        gcp.list_sheet_tabs,
        gcp.update_sheet_tab,
        gcp.create_spreadsheet_in_folder,
    ):
        retrying: ta.Any = getattr(fn, "retry", None)
        if retrying is not None:
            monkeypatch.setattr(retrying, "wait", tenacity.wait_none())


def _mock_sheets_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Install a mock Sheets client (MagicMock so ``cache_clear`` exists)."""
    mock_client = MagicMock()
    monkeypatch.setattr(gcp, "__sheets_client", MagicMock(return_value=mock_client))
    return mock_client


@pytest.mark.parametrize(
    "transient",
    [_http_error(503), _http_error(429), TimeoutError("The read operation timed out")],
    ids=["http_503", "http_429", "socket_timeout"],
)
def test_read_sheet_tab_retries_transient_failures(
    monkeypatch: pytest.MonkeyPatch,
    no_retry_sleep: None,
    transient: BaseException,
) -> None:
    """A single transient Sheets failure is retried, not surfaced to callers."""
    mock_client = _mock_sheets_client(monkeypatch)
    execute = (
        mock_client.spreadsheets.return_value.values.return_value.get.return_value.execute
    )
    execute.side_effect = [
        transient,
        {"values": [["sector", "N_new"], ["1111A0", "1.5"]]},
    ]

    df = gcp.read_sheet_tab("sheet123", "N_and_diffs")

    assert execute.call_count == 2
    assert list(df.columns) == ["sector", "N_new"]
    assert df["N_new"].tolist() == ["1.5"]


def test_read_sheet_tab_does_not_retry_client_errors(
    monkeypatch: pytest.MonkeyPatch,
    no_retry_sleep: None,
) -> None:
    """A 404 (bad sheet id / missing tab) is a real error — fail fast."""
    mock_client = _mock_sheets_client(monkeypatch)
    execute = (
        mock_client.spreadsheets.return_value.values.return_value.get.return_value.execute
    )
    execute.side_effect = _http_error(404)

    with pytest.raises(HttpError):
        gcp.read_sheet_tab("sheet123", "N_and_diffs")

    assert execute.call_count == 1


def test_read_sheet_tab_reraises_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch,
    no_retry_sleep: None,
) -> None:
    """A persistently unavailable API still raises, after the attempt budget."""
    mock_client = _mock_sheets_client(monkeypatch)
    execute = (
        mock_client.spreadsheets.return_value.values.return_value.get.return_value.execute
    )
    execute.side_effect = _http_error(503)

    with pytest.raises(HttpError):
        gcp.read_sheet_tab("sheet123", "N_and_diffs")

    assert execute.call_count == gcp._RETRY_ATTEMPTS


def test_create_spreadsheet_in_folder_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
    no_retry_sleep: None,
) -> None:
    """Non-idempotent writes stay unretried: a retry could duplicate the sheet."""
    mock_client = MagicMock()
    mock_client.files.return_value.create.return_value.execute.side_effect = (
        _http_error(503)
    )
    monkeypatch.setattr(gcp, "__drive_client", MagicMock(return_value=mock_client))

    with pytest.raises(HttpError):
        gcp.create_spreadsheet_in_folder(title="run", folder_id="folder_abc")

    assert mock_client.files.return_value.create.return_value.execute.call_count == 1


def test_update_sheet_tab_retry_does_not_re_add_the_tab(
    monkeypatch: pytest.MonkeyPatch,
    no_retry_sleep: None,
) -> None:
    """Retrying a write is idempotent: the second pass sees the created tab.

    Without this, a timeout after ``addSheet`` landed would make the retry
    re-add an existing title and fail with a 400.
    """
    mock_client = _mock_sheets_client(monkeypatch)
    spreadsheets = mock_client.spreadsheets.return_value
    spreadsheets.get.return_value.execute.side_effect = [
        {"sheets": []},
        {"sheets": [{"properties": {"title": "my_tab"}}]},
    ]
    spreadsheets.values.return_value.clear.return_value.execute.side_effect = [
        TimeoutError("The read operation timed out"),
        {},
    ]

    gcp.update_sheet_tab("sheet123", "my_tab", pd.DataFrame({"a": [1]}))

    assert spreadsheets.batchUpdate.call_count == 1
    assert spreadsheets.values.return_value.update.return_value.execute.call_count == 1


def test_update_sheet_tab_treats_unavailable_metadata_as_retryable(
    monkeypatch: pytest.MonkeyPatch,
    no_retry_sleep: None,
) -> None:
    """A 503 on the tab-listing read must not be read as "tab missing"."""
    mock_client = _mock_sheets_client(monkeypatch)
    spreadsheets = mock_client.spreadsheets.return_value
    spreadsheets.get.return_value.execute.side_effect = [
        _http_error(503),
        {"sheets": [{"properties": {"title": "my_tab"}}]},
    ]

    gcp.update_sheet_tab("sheet123", "my_tab", pd.DataFrame({"a": [1]}))

    assert spreadsheets.batchUpdate.call_count == 0

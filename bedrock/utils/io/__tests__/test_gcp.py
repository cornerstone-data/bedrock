from unittest.mock import MagicMock

import pytest

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

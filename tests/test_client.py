from unittest.mock import MagicMock, patch

from client import SageIntacctClient, SageIntacctClientConfig


def test_client_credentials_authenticate():
    config = SageIntacctClientConfig(
        client_id="test_client_id",
        client_secret="test_client_secret",
        username="admin@company123",
    )

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "token_type": "Bearer",
        "access_token": "test_access_token",
        "expires_in": 43200,
    }
    mock_response.raise_for_status.return_value = None

    with patch("requests.Session") as MockSession:
        mock_session = MockSession.return_value
        mock_session.post.return_value = mock_response

        client = SageIntacctClient(config)

        mock_session.post.assert_called_once_with(
            "https://api.intacct.com/ia/api/v1/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "username": "admin@company123",
            },
            timeout=30,
        )
        assert client._access_token == "test_access_token"

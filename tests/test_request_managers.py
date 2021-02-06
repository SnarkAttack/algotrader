from coinbase_pro_bot.api_request_manager import (
    PublicAPIRequestManager,
    AuthenticatedAPIRequestManager,
    WebsocketManager
)

def test_public_api_request_manager():
    req_man = PublicAPIRequestManager()
    assert req_man.client is not None
    response = req_man.client.get_currencies()
    req_man.shutdown_client()
    assert req_man.client is None


def test_authenticated_api_request_manager():
    req_man = AuthenticatedAPIRequestManager('tests/test_keyfile.txt')
    assert req_man.client is not None
    req_man.shutdown_client()
    assert req_man.client is None


def test_websocket_api_request_manager():
    req_man = WebsocketManager()
    assert req_man.client is not None
    req_man.shutdown_client()
    assert req_man.client is None

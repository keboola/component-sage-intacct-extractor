from configuration import Configuration


def test_configuration_requires_username():
    cfg = Configuration(authorization={"username": "admin@mycompany|US"})
    assert cfg.authorization.username == "admin@mycompany|US"


def test_configuration_username_defaults_empty():
    cfg = Configuration()
    assert cfg.authorization.username == ""


def test_configuration_client_id_alias():
    cfg = Configuration(authorization={"#client_id": "my_client_id", "#client_secret": "my_secret", "username": "u@c"})
    assert cfg.authorization.client_id == "my_client_id"
    assert cfg.authorization.client_secret == "my_secret"

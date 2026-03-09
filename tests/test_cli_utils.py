from gdrive_dedupe.cli.app import _parse_size_to_bytes


def test_parse_size_to_bytes_units() -> None:
    assert _parse_size_to_bytes("0") == 0
    assert _parse_size_to_bytes("512") == 512
    assert _parse_size_to_bytes("1KB") == 1024
    assert _parse_size_to_bytes("1.5MB") == int(1.5 * 1024 * 1024)
    assert _parse_size_to_bytes("2GB") == 2 * 1024 * 1024 * 1024

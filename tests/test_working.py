import miniq


def test_package_imports() -> None:
    assert miniq.__name__ == "miniq"

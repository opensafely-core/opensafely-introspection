import pathlib

import src


def test_root_dir():
    assert pathlib.Path(__file__).parents[1] == src.ROOT_DIR

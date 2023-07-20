#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License


import os

import pytest
from IPython.testing.globalipapp import get_ipython

from sparglim.exceptions import UnconfigurableError
from sparglim.sql.magic import SparkMagic

_HERE = os.path.dirname(__file__)
example_file = os.path.join(_HERE, "example/people.json")


ipy = get_ipython()
ipy.run_line_magic("load_ext", "sparglim.sql")


def test_sql():
    ipy.run_line_magic("sql", "SHOW DATABASES")


def test_line():
    ipy.run_line_magic("sql", f'CREATE TABLE tb_people USING json OPTIONS (path "{example_file}")')


def test_cell():
    ipy.run_cell_magic(
        "sql", "CREATE TABLE tb_people2", f' USING json OPTIONS (path "{example_file}")'
    )


def test_mix():
    ipy.run_cell_magic(
        "sql",
        "CREATE TABLE tb_people3",
        f'USING json OPTIONS (path "{example_file}");' "SELECT * FROM tb_people3",
    )
    ipy.run_line_magic("sql", "SHOW TABLES")


@pytest.mark.parametrize("mode", ["local", "k8s", "connect_client", "connect_server"])
def test_env_config(monkeypatch, mode, k8s_config_path):
    monkeypatch.setenv(SparkMagic.ENV_MASTER_MODE, mode)
    SparkMagic.clear()
    if mode == "connect_server":
        with pytest.raises(UnconfigurableError):
            SparkMagic(k8s_config_path=k8s_config_path)
    else:
        SparkMagic(k8s_config_path=k8s_config_path)


if __name__ == "__main__":
    pytest.main(["-vv", "-s", __file__])

#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License


import os

import pytest
from IPython.testing.globalipapp import get_ipython

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


if __name__ == "__main__":
    pytest.main(["-vv", "-s", __file__])

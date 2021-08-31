from shutil import copyfile

import pytest

from Services import FileHandler


@pytest.fixture
def filehandler(tmp_path):
    tmp = tmp_path
    copyfile("tests/Data/hotels.zip", tmp / "hotels.zip")
    return FileHandler(tmp, tmp / "Output")


def test_filehandler_delete_temp(filehandler):
    fl = filehandler
    temp_path = fl.temp_path
    fl.__del__()
    assert not temp_path.exists()


def test_filehandler_unzip_files(filehandler):
    fl = filehandler
    assert sum([1 for f in fl.temp_path.iterdir()]) == 5


def test_filehandler_read_and_concat_csv(filehandler):
    fl = filehandler
    assert len(fl.read_csv()) == 2494


def test_filehandler_clear_rows(filehandler):
    fl = filehandler
    assert len(fl.hotels_df) == 2302


@pytest.mark.parametrize("num", ["5", "0", "2.5", "235.154"])
def test_filehandler_is_float_true(num):
    assert FileHandler.is_float(num)


@pytest.mark.parametrize("num", ["5a", "2,5", "asd4.5"])
def test_filehandler_is_float_false(num):
    assert not FileHandler.is_float(num)

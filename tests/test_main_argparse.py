from pathlib import Path, WindowsPath

from main import arg_parser


def test_command_line_parser_indir():
    parser = arg_parser(["test"])
    assert Path(parser.indir) == Path("test")


def test_command_line_parser_outdir():
    parser = arg_parser(["test", "--outdir=output"])
    assert Path(parser.outdir) == Path("output")


def test_command_line_parser_threads():
    parser = arg_parser(["test", "--threads=15"])
    assert parser.threads == 15


def test_command_line_parser_threads_default():
    parser = arg_parser(["test"])
    assert parser.threads == 100


def test_command_line_parser_hotels():
    parser = arg_parser(["test", "--hotels=7"])
    assert parser.hotels == 7

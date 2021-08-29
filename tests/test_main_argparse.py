from pathlib import WindowsPath

from main import command_line_parser


def test_command_line_parser_indir():
    parser = command_line_parser(["test"])
    assert parser.indir == WindowsPath("test")


def test_command_line_parser_outdir():
    parser = command_line_parser(["test", "-outdir output"])
    assert parser.outdir == WindowsPath("output")

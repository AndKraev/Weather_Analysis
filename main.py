import argparse
import sys

from WeatherAnalysis import WeatherAnalysis


def arg_parser(args) -> argparse:
    """Function to obtain parameters from console to run the code

    :param args: args for testing
    :return: parameters from console
    :type: argparse.parse_args
    """
    parser = argparse.ArgumentParser(
        prog="Incredible Hotel Weather Analyzer",
        description="""Unzips csv files from the input folder, find cities with most 
        hotels in each country and writes data to output folder""",
    )

    parser.add_argument("indir", type=str, help="a path to an input folder with hotels")

    parser.add_argument(
        "--outdir",
        type=str,
        help="a path to an output folder with results (by default creates 'Output' "
        "folder in the input folder)",
    )

    parser.add_argument(
        "--threads",
        type=int,
        default=1000,
        help="a maximum number of threads to be used (default=1000)",
    )

    parser.add_argument(
        "--hotels",
        type=int,
        default=100,
        help="a maximum number of hotels to be written (default=100)",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    WeatherAnalysis(*vars(arg_parser(sys.argv[1:])).values()).run()
    print("Completed!")

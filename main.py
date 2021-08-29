import argparse
from pathlib import Path

from WeatherAnalysis import WeatherAnalysis


def command_line_parser():
    parser = argparse.ArgumentParser(prog='Incredible Hotel Weather Analyzer',
                                     description="""Unzips csv files from the input
                                                     folder, find cities with most
                                                     hotels in each country and writes
                                                     data to output folder""")

    parser.add_argument('indir', type=Path,
                        help='a path to an input folder with hotels')

    parser.add_argument('--outdir', type=Path,
                        help='a path to an output folder with results '
                             '(by default creates "Output" folder in the input folder)')

    parser.add_argument('--threads', type=int, default=100,
                        help='a maximum number of threads to be used (default=100)')

    parser.add_argument('--hotels', type=int, default=3,
                        help='a maximum number of hotels to be written (default=100)')

    return vars(parser.parse_args())


if __name__ == "__main__":
    args = command_line_parser()

    WeatherAnalysis(
        indir=args['indir'],
        outdir=args['outdir'],
        max_hotels=args['hotels'],
        threads=args['threads']
    )

    print("Completed!")


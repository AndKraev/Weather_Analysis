import csv
import tempfile
from pathlib import Path
import zipfile


class FileHandler:
    def __init__(self, input_folder, output_folder=None):
        # self.temp_folder = tempfile.TemporaryFile()
        # self.temp_path = Path(self.temp_folder.name)
        self.input_path = Path(input_folder)
        self.output_path = (
            Path(output_folder) if output_folder else self.input_path / "Output"
        )

    def unzip_files(self):
        for file in self.input_path.iterdir():
            if zipfile.is_zipfile(file):
                with zipfile.ZipFile(file, mode="r") as archive:
                    for zipped_file in archive.namelist():
                        if zipped_file.endswith(".csv"):
                            self.read_csv(zipped_file)

    def read_csv(self, file):
        with open(file, mode = "r", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                if all([row["Name"], row["Latitude"], row["Longitude"]]):
                    self.write_csv(row)

    def write_csv(self, row):
        pass


if __name__ == "__main__":
    hotels = FileHandler(r"D:\PyProjects\Weather_Analysis\Data")
    hotels.unzip_files()

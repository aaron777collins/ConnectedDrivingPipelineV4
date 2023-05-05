from EasyMLLib.CSVWriter import CSVWriter
import base64

class ImageWriter:

    def __init__(self, csvWriter: CSVWriter):
        self.csvWriter = csvWriter

    def writeHeaders(self, headers: list[str]):
        numCols = len(self.csvWriter.columns)
        headers.extend([""] * (numCols - len(headers)))
        self.csvWriter.addRow(headers)

    def writeImage(self, image, label: str):
        self.csvWriter.addRow([label, image])

    def readImageAsBase64(self, imagepath: str):
        with open(imagepath, "rb") as imageFile:
            # read image as base64
            return base64.b64encode(imageFile.read()).decode('utf-8')

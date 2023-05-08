from EasyMLLib.CSVWriter import CSVWriter
import base64

class ImageWriter:

    def __init__(self, csvWriter: CSVWriter):
        self.csvWriter = csvWriter

    def writeRow(self, headers: list[str]):
        numCols = len(self.csvWriter.columns)
        headers.extend([""] * (numCols - len(headers)))
        self.csvWriter.addRow(headers)

    def writeImageAtEndOfRow(self, rowCells: list[str], imageArr: list[str]):
        numCols = len(self.csvWriter.columns)
        # write row cells and then image array at the end.
        rowCells.extend(imageArr)
        rowCells.extend([""] * (numCols - len(rowCells) - len(imageArr)))
        self.csvWriter.addRow(rowCells)

    def readImageAsBase64Array(self, imagepath: str):
        with open(imagepath, "rb") as imageFile:
            # read image as base64
            ext = imagepath.split(".")[-1]
            base64_utf8_str = base64.b64encode(imageFile.read()).decode('utf-8')
            dataurl = f'data:image/{ext};base64,{base64_utf8_str}'
            # split the dataurl into an array of 49999 characters each
            dataurlarr = [dataurl[i:i + 49999] for i in range(0, len(dataurl), 49999)]
            return dataurlarr

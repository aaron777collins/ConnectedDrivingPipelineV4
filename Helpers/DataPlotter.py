import os
from pandas import DataFrame
import matplotlib.pyplot as plt

from Helpers.ImageWriter import ImageWriter


class DataPlotter:
    @staticmethod
    def plot_data(self, data: DataFrame, x: str, y: str, title: str, extraColumnsArgs: list[str] = []):
        # reset plot
        plt.clf()
        # xlim as min and max of x
        plt.xlim(data[x].min(), data[x].max())
        # ylim as min and max of y
        plt.ylim(data[y].min(), data[y].max())
        plt.scatter(data[x], data[y])
        plt.title(title)
        plt.xlabel(x)
        plt.ylabel(y)
        # save to the plots folder
        plotPath = self._mlPathProvider.getPathWithModelName(f"MDataClassifier.plot_distribution_path")

        finalPlotPath = plotPath + f"{title}.png"

        os.makedirs(plotPath, exist_ok=True)

        plt.savefig(finalPlotPath)

        # write image to the csv
        imageWriter = ImageWriter(self.csvWriter)
        imageWriter.writeImageAtEndOfRow([title,  len(data)].extend(extraColumnsArgs), imageWriter.readImageAsBase64Array(finalPlotPath))

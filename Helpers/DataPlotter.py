import os
from pandas import DataFrame
import matplotlib.pyplot as plt

from Helpers.ImageWriter import ImageWriter


class DataPlotter:
    @staticmethod
    def plot_data(self, data: DataFrame, x: str, y: str, title: str):
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
        # date = mm/dd/yyyy from the settings
        day = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.day")
        month = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.month")
        year = self.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.year")
        # date has filler 0s if needed ex. 1 -> 01
        daystr = str(day) if day >= 10 else f"0{day}"
        monthstr = str(month) if month >= 10 else f"0{month}"
        yearstr = str(year)
        date = f"{monthstr}/{daystr}/{yearstr}"
        imageWriter.writeImageAtEndOfRow([title,  len(data), date], imageWriter.readImageAsBase64Array(finalPlotPath))

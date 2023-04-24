from Gatherer.IDataGatherer import IDataGatherer
from Logger.Logger import Logger
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IKeyProvider import IKeyProvider
from ServiceProviders.IPathProvider import IPathProvider


class DataGatherer(IDataGatherer):

        def __init__(self, pathprovider: IPathProvider, contextprovider: IDictProvider):
            self._pathprovider = pathprovider()
            self._contextprovider = contextprovider()
            self.logger = Logger("DataGatherer", self._pathprovider.__class__)

            self.data = None
            self.numrows = self._contextprovider.get("DataGatherer.numrows")
            self.filepath = self._pathprovider.getPathWithModelName("DataGatherer.filepath")
            self.subsectionpath = self._pathprovider.getPathWithModelName("DataGatherer.subsectionpath")
            self.subsectionname = self._contextprovider.get("DataGatherer.subsectionname")
            self.splitfilespath = self._pathprovider.getPathWithModelName("DataGatherer.splitfilespath")

        def gatherData(self):
            pass

        def getGatheredData(self):
            pass

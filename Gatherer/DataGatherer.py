from Gatherer.IDataGatherer import IDataGatherer
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IPathProvider import IPathProvider


class DataGatherer(IDataGatherer):

        def __init__(self, pathprovider: IPathProvider, contextprovider: IDictProvider):
            _pathprovider = pathprovider()
            _contextprovider = contextprovider()
            self.data = None
            self.numrows = contextprovider.get("numrows")
            self.filepath = pathprovider.getPathWithModelName(filepathkey)
            self.subsectionpath = pathprovider.getPathWithModelName(subsectionpathkey)

        def gatherData(self):
            pass

        def getGatheredData(self):
            pass

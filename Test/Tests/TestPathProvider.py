from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest


class TestPathProvider(ITest):

    def run(self):
        # test class

        pp = PathProvider(paths={"test2": "test2"})

        pp.addPath("test", "test")

        assert(pp.getPaths()["test"] == "test")

        pp.removePath("test")

        assert(pp.getPaths() == {"test2": "test2"})

        pp2 = PathProvider()

        pp2.addPath("test", "test")

        assert(pp2.getPaths()["test"] == "test")

        pp2.removePath("test")

        assert(pp2.getPaths() == {"test2": "test2"})

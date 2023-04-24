from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest


class TestPathProvider(ITest):

    def run(self):
        # add data to dict provider to test overlap
        dp = DictProvider(contexts={"test2": "test2"})

        dp.add("test", "test")

        pp = PathProvider(model="test")
        assert(pp.getAllPathsWithModelName() == {})

        assert(dp.getAll() == {"test2": "test2", "test": "test"})

        pp.add("somepath", lambda model: f"somepath/{model}")

        assert(pp.getAllPathsWithModelName() == {"somepath": "somepath/test"})
        assert(pp.getPathWithModelName("somepath") == "somepath/test")

        PathProvider.clear()

        # add path provider without model
        pp = PathProvider()
        # add data to dict
        pp.add("test", lambda model: f"test/{model}")

        assert(pp.getAllPathsWithModelName() == {"test": f"test/{PathProvider.DEFAULT_MODEL_NAME}"})

    def cleanup(self):

        DictProvider.clear()
        PathProvider.clear()

from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.KeyProvider import KeyProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest


class TestKeyProvider(ITest):

    def run(self):
        # pass
        # adding data to the other providers to test overlap
        dp = DictProvider(contexts={"test2": "test2"})

        dp.add("test", "test")

        pp = PathProvider(model="test")
        assert(pp.getAllPathsWithModelName() == {})

        assert(dp.getAll() == {"test2": "test2", "test": "test"})

        pp.add("somepath", lambda model: f"somepath/{model}")

        assert(pp.getAllPathsWithModelName() == {"somepath": "somepath/test"})
        assert(pp.getPathWithModelName("somepath") == "somepath/test")

        kp = KeyProvider()

        assert(kp.getAll() == {})

        # test key provider

        kp.add("test", "test")

        assert(kp.getAll() == {"test": "test"})


        DictProvider.clear()
        PathProvider.clear()
        KeyProvider.clear()

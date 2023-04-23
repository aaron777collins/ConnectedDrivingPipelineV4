from ServiceProviders.DictProvider import DictProvider
from Test.ITest import ITest


class TestDictProvider(ITest):

    def run(self):
        # test class

        dp = DictProvider(contexts={"test2": "test2"})

        dp.add("test", "test")

        assert(dp.get("test") == "test")

        dp.remove("test")

        assert(dp.get("test2") ==  "test2")

        dp2 = DictProvider()

        dp2.add("test", "test")

        assert(dp2.get("test") == "test")

        dp2.remove("test")

        assert(dp2.get("test2") ==  "test2")

        DictProvider.clear()
        # new instance
        dp = DictProvider()
        # ensure empty
        assert(dp.getAll() == {})
        # clear again
        DictProvider.clear()

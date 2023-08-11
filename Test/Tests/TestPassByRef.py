from ServiceProviders.DictProvider import DictProvider
from Test.ITest import ITest


class TestPassByRef(ITest):

    def test_func_to_add_to_dict(self, someDict):
        someDict["SomeValue"] = "SomeValue1"

    def run(self):

        dp = DictProvider(contexts={"thedict": dict()})

        dict1 = dp.get("thedict")

        assert(dict1.get("SomeValue", "NA") == "NA")

        self.test_func_to_add_to_dict(dict1)

        assert(dict1.get("SomeValue", "NA") == "SomeValue1")

        dict2 = dp.get("thedict")
        assert(dict2.get("SomeValue", "NA") == "SomeValue1")


    def cleanup(self):
        DictProvider.clear()

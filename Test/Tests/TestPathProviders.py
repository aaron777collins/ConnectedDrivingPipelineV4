from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider
from Test.ITest import ITest


class TestPathProviders(ITest):

    def run(self):
        # add data to dict provider to test overlap
        dp = DictProvider(contexts={"test2": "test2"})

        dp.add("test", "test")

        pp = PathProvider(model="test")
        assert(pp.getAllPathsWithModelName() == {})

        igpp = InitialGathererPathProvider(model="test")
        assert(igpp.getAllPathsWithModelName() == {})

        gpp = GeneratorPathProvider(model="test")
        assert(gpp.getAllPathsWithModelName() == {})

        mlpp = MLPathProvider(model="test")
        assert(mlpp.getAllPathsWithModelName() == {})

        assert(dp.getAll() == {"test2": "test2", "test": "test"})

        pp.add("somepath", lambda model: f"somepath/{model}")
        igpp.add("somepath2", lambda model: f"somepath2/{model}")
        gpp.add("somepath3", lambda model: f"somepath3/{model}")
        mlpp.add("somepath4", lambda model: f"somepath4/{model}")

        assert(pp.getAllPathsWithModelName() == {"somepath": "somepath/test"})
        assert(pp.getPathWithModelName("somepath") == "somepath/test")

        assert(igpp.getAllPathsWithModelName() == {"somepath2": "somepath2/test"})
        assert(igpp.getPathWithModelName("somepath2") == "somepath2/test")

        assert(gpp.getAllPathsWithModelName() == {"somepath3": "somepath3/test"})
        assert(gpp.getPathWithModelName("somepath3") == "somepath3/test")

        assert(mlpp.getAllPathsWithModelName() == {"somepath4": "somepath4/test"})
        assert(mlpp.getPathWithModelName("somepath4") == "somepath4/test")

        PathProvider.clear()
        InitialGathererPathProvider.clear()
        GeneratorPathProvider.clear()
        MLPathProvider.clear()

        # add path provider without model
        pp = PathProvider()
        igpp = InitialGathererPathProvider()
        gpp = GeneratorPathProvider()
        mlpp = MLPathProvider()

        # add data to dict
        pp.add("test", lambda model: f"test/{model}")
        igpp.add("test2", lambda model: f"test2/{model}")
        gpp.add("test3", lambda model: f"test3/{model}")
        mlpp.add("test4", lambda model: f"test4/{model}")

        assert(pp.getAllPathsWithModelName() == {"test": f"test/{PathProvider.DEFAULT_MODEL_NAME}"})
        assert(igpp.getAllPathsWithModelName() == {"test2": f"test2/{InitialGathererPathProvider.DEFAULT_MODEL_NAME}"})
        assert(gpp.getAllPathsWithModelName() == {"test3": f"test3/{GeneratorPathProvider.DEFAULT_MODEL_NAME}"})
        assert(mlpp.getAllPathsWithModelName() == {"test4": f"test4/{MLPathProvider.DEFAULT_MODEL_NAME}"})

    def cleanup(self):

        DictProvider.clear()
        PathProvider.clear()
        InitialGathererPathProvider.clear()
        GeneratorPathProvider.clear()
        MLPathProvider.clear()

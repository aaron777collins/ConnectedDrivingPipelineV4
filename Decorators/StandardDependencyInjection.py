from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.IDictProvider import IDictProvider
from ServiceProviders.IGeneratorContextProvider import IGeneratorContextProvider
from ServiceProviders.IGeneratorPathProvider import IGeneratorPathProvider
from ServiceProviders.IInitialGathererPathProvider import IInitialGathererPathProvider
from ServiceProviders.IKeyProvider import IKeyProvider
from ServiceProviders.IMLContextProvider import IMLContextProvider
from ServiceProviders.IMLPathProvider import IMLPathProvider
from ServiceProviders.IPathProvider import IPathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.KeyProvider import KeyProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.MLPathProvider import MLPathProvider
from ServiceProviders.PathProvider import PathProvider


class StandardDependencyInjection(object):

    SDI_DEPENDENCIES = {

        IDictProvider.__name__: DictProvider,
        IKeyProvider.__name__: KeyProvider,
        IGeneratorContextProvider.__name__: GeneratorContextProvider,
        IMLContextProvider.__name__: MLContextProvider,
        IPathProvider.__name__: PathProvider,
        IInitialGathererPathProvider.__name__: InitialGathererPathProvider,
        IGeneratorPathProvider.__name__: GeneratorPathProvider,
        IMLPathProvider.__name__: MLPathProvider,
    }

    def __init__(self, class_to_instantiate):
        self.class_to_instantiate = class_to_instantiate

    def __call__(self, *args, **kwargs):

        # get the classes' arguments and
        # check if any of them are of the type of the keys in the dictionary
        # if so then set the argument in args to the value in the dictionary
        # zip(self.class_to_instantiate.__init__.__annotations__.values().insert(0, self.class_to_instantiate), self.class_to_instantiate.__init__.__code__.co_varnames)
        print(self.class_to_instantiate.__init__.__annotations__.values())
        print(self.class_to_instantiate.__init__.__annotations__)
        args2 = args
        for argtype in self.class_to_instantiate.__init__.__annotations__.values():
            if argtype.__name__ in self.SDI_DEPENDENCIES:
                args2 = args2 + tuple([self.SDI_DEPENDENCIES[argtype.__name__]])

        return self.class_to_instantiate(*args2, **kwargs)

    def cleanup(self):
        # clear dependencies by calling the clear method on each of the values in the dictionary
        for dependency in self.SDI_DEPENDENCIES.values():
            dependency.clear()

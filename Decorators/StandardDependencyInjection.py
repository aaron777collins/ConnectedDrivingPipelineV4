from functools import wraps
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

def StandardDependencyInjection(func_to_decorate):
    @wraps(func_to_decorate)
    def wrapper(*args, **kwargs):
        args2 = args
        for argtype in func_to_decorate.__annotations__.values():
            if argtype.__name__ in SDI_DEPENDENCIES:
                args2 = args2 + tuple([SDI_DEPENDENCIES[argtype.__name__]])
        return func_to_decorate(*args2, **kwargs)
    return wrapper


def cleanup(self):
    # clear dependencies by calling the clear method on each of the values in the dictionary
    for dependency in self.SDI_DEPENDENCIES.values():
        dependency.clear()

from ServiceProviders.IKeyProvider import IKeyProvider


class KeyProvider(IKeyProvider):

    def __init__(self, contexts=None):
        self.contexts: dict[str, str] = contexts
        if contexts is None:
            self.contexts: dict[str, str] = {}

    def get(self, key):
        return self.contexts[key]

    def set(self, contexts: dict[str,str]):
        self.contexts = contexts

    def add(self, key, context):
        self.contexts[key] = context

    def remove(self, key):
        del self.contexts[key]

    def getAll(self):
        return self.contexts

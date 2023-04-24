from abc import ABC, abstractmethod

class ITest:

    @abstractmethod
    def run():
        pass

    def cleanup():
        pass

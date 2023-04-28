# Create a class Logger to log the info of the program with an elevation level, prefix and message.
import os
from datetime import datetime
from Decorators.StandardDependencyInjection import StandardDependencyInjection
from Logger.LogLevel import LogLevel

from ServiceProviders.IPathProvider import IPathProvider

DEFAULT_LOG_PATH = lambda model: f"logs/{model}/"

class Logger:
    @StandardDependencyInjection
    def __init__(self, prefix, pathprovider: IPathProvider):
        self._pathprovider = pathprovider()
        self.logpath = self._pathprovider.getPathWithModelName("Logger.logpath")
        self.prefix = prefix

    def log(self, *messages, elevation=LogLevel.INFO):
        # print to console and append to file
        messages = (message if isinstance(message, str) else str(message) for message in messages)
        string = f"{elevation} {datetime.now().strftime('%m/%d/%Y, %H:%M:%S')} {self.prefix}: {' '.join(messages)}"
        print(string)
        os.makedirs(os.path.dirname(self.logpath), exist_ok=True)
        with open(f"{self.logpath}{self.prefix}.txt", "a") as f:
            f.write(string + "\n")

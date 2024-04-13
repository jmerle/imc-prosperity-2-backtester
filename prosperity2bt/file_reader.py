from abc import abstractmethod
from contextlib import contextmanager
from importlib import resources
from pathlib import Path
from typing import ContextManager, Optional

@contextmanager
def wrap_in_context_manager(value):
    yield value

class FileReader:
    @abstractmethod
    def file(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        """Given a path to a file, yields a single Path object to the file or None if the file does not exist."""
        raise NotImplementedError()

class FileSystemReader(FileReader):
    def __init__(self, root: Path) -> None:
        self._root = root

    def file(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        file = self._root
        for part in path_parts:
            file = file / part

        if not file.is_file():
            return wrap_in_context_manager(None)

        return wrap_in_context_manager(file)

class PackageResourcesReader(FileReader):
    def file(self, path_parts: list[str]) -> ContextManager[Optional[Path]]:
        try:
            container = resources.files(f"prosperity2bt.resources.{'.'.join(path_parts[:-1])}")
            if not (container / path_parts[-1]).is_file():
                return wrap_in_context_manager(None)

            return resources.as_file(container / path_parts[-1])
        except:
            return wrap_in_context_manager(None)

from dataclasses import dataclass

from .lance import (
    _Dataset,
)

@dataclass
class Tags:
    _ds: _Dataset

    def list(self) -> dict[str, int]:
        """
        Return all tags in this dataset.
        """
        return self._ds.tags()

    def create(self, tag: str, version: int) -> None:
        """
        Create a tag for a given dataset version.
        """
        self._ds.create_tag(tag, version)

    def delete(self, tag: str) -> None:
        """
        Delete tag from the dataset.
        """
        self._ds.delete_tag(tag)

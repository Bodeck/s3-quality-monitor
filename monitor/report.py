from dataclasses import dataclass

from monitor.validators import ValidationResults

@dataclass
class FileReport:
    key: str
    results: list[ValidationResults]
    err: str | None = None
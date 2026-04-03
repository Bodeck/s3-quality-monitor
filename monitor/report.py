from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
import json

from monitor.validators import ValidationResults


@dataclass
class FileReport:
    key: str
    results: list[ValidationResults]
    error: str | None = None


@dataclass
class SummaryReport:
    files: list[FileReport]
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    
    @property
    def files_scanned(self) -> int:
        return len(self.files)
    
    @property
    def files_passed(self) -> int:
        return sum(1 for f in self.files if f.error is None and all(r.passed for r in f.results))
    
    @property
    def files_failed(self) -> int:
        return self.files_scanned - self.files_passed

    def to_json(self, indent: int = 2) -> str:
        data = asdict(self)
        data["timestamp"] = self.timestamp.isoformat()

        return json.dumps(obj=data, indent=indent)

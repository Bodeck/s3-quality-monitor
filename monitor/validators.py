from abc import ABC, abstractmethod
from dataclasses import dataclass
from pandas import DataFrame


@dataclass
class ValidationResults:
    check_name: str
    passed: bool = False
    details: str = ""


class Validator(ABC):
    def __init__(self) -> None:
        self.check_name = self.__class__.__name__
        super().__init__()

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResults: ...


class NullRatioCheck(Validator):
    def __init__(self, threshold: float, column: str | None = None) -> None:
        self.threshold = threshold
        self.column = column
        super().__init__()

    def validate(self, df: DataFrame) -> ValidationResults:
        results = ValidationResults(check_name=self.check_name)

        if self.column is None:
            ratio = df.isnull().mean().mean()
        elif self.column in df.columns:
            series = df[self.column]
            ratio = series.isnull().mean()
        else:
            results.details = f"Column '{self.column}' does not exists in dataframe. Available: {list(df.columns)}"
            return results

        if ratio > self.threshold:
            results.details = (
                f"Null ratio {ratio:.2%} exceeds {self.threshold:.2%} threshold."
            )
        else:
            results.passed = True

        return results


class SchemaCheck(Validator):
    def __init__(self, expected_columns: list[str]) -> None:
        self.expected_columns = expected_columns
        super().__init__()

    def validate(self, df: DataFrame) -> ValidationResults:
        if set(self.expected_columns).issubset(df.columns):
            return ValidationResults(check_name=self.check_name, passed=True)

        return ValidationResults(
            check_name=self.check_name,
            passed=False,
            details="File is missing required columns",
        )


class RowCountCheck(Validator):
    def __init__(self, min_rows: int) -> None:
        self.min_rows = min_rows
        super().__init__()

    def validate(self, df: DataFrame) -> ValidationResults:
        if len(df) < self.min_rows:
            return ValidationResults(
                check_name=self.check_name,
                details=f"Minimal number of rows is {self.min_rows}",
            )
        return ValidationResults(check_name=self.check_name, passed=True)

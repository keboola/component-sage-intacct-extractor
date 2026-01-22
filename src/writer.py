import csv


class SageIntacctWriter:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._file = None
        self._writer = None
        self._columns = None

    def writerows(self, rows: list[dict]):
        if not rows:
            return

        if not self._columns:
            self._columns = list(rows[0].keys())
            self._file = open(self.file_path, "w", newline="", encoding="utf-8")
            self._writer = csv.DictWriter(self._file, fieldnames=self._columns)
            self._writer.writeheader()

        if self._writer:
            self._writer.writerows(rows)

    def get_result_columns(self) -> list[str]:
        return self._columns or []

    def close(self):
        if self._file:
            self._file.close()

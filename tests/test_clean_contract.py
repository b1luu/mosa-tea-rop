import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "src" / "clean.py"


def write_csv(path: Path, rows, columns):
    df = pd.DataFrame(rows, columns=columns)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


class CleanPipelineContractTests(unittest.TestCase):
    def test_fixed_ice_and_hot_no_ice_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            raw_path = tmp_path / "raw.csv"
            out_path = tmp_path / "clean.csv"

            write_csv(
                raw_path,
                [
                    # Should default to 100% Ice.
                    [
                        "2026-01-01",
                        "12:00:00",
                        "Matcha Series",
                        "Matcha Latte",
                        1,
                        "25% Sugar",
                        "Payment",
                        "tx-1",
                    ],
                    # Existing fixed-ice rule should still work.
                    [
                        "2026-01-01",
                        "12:05:00",
                        "Matcha Series",
                        "Strawberry Matcha Latte",
                        1,
                        "No Sugar",
                        "Payment",
                        "tx-2",
                    ],
                    # Hot drinks with no explicit ice token should default to 0% ice.
                    [
                        "2026-01-01",
                        "12:10:00",
                        "Seasonal Special",
                        "Hot Spice Apple Tea Cider",
                        1,
                        "50% Sugar",
                        "Payment",
                        "tx-3",
                    ],
                    # Refund should be excluded.
                    [
                        "2026-01-01",
                        "12:15:00",
                        "Matcha Series",
                        "Matcha Latte",
                        -1,
                        "25% Sugar",
                        "Refund",
                        "tx-4",
                    ],
                ],
                [
                    "Date",
                    "Time",
                    "Category",
                    "Item",
                    "Qty",
                    "Modifiers Applied",
                    "Event Type",
                    "Transaction ID",
                ],
            )

            subprocess.run(
                [
                    "python3",
                    str(SCRIPT_PATH),
                    "--input",
                    str(raw_path),
                    "--output",
                    str(out_path),
                ],
                check=True,
                cwd=str(REPO_ROOT),
            )

            cleaned = pd.read_csv(out_path, low_memory=False)

            # Refund row removed: only three payment rows remain.
            self.assertEqual(len(cleaned), 3)

            # No missing ice_pct after defaults are applied.
            self.assertFalse(cleaned["ice_pct"].isna().any())

            # Matcha Latte defaults to 100% ice and backfills modifier token.
            matcha = cleaned[cleaned["Item"].eq("Matcha Latte")].iloc[0]
            self.assertEqual(int(matcha["ice_pct"]), 100)
            self.assertIn("100% Ice", str(matcha["Modifiers Applied"]))

            # Strawberry Matcha Latte remains fixed at 100% ice.
            strawberry = cleaned[cleaned["Item"].eq("Strawberry Matcha Latte")].iloc[0]
            self.assertEqual(int(strawberry["ice_pct"]), 100)

            # Hot spice cider gets No Ice / 0 ice defaults.
            hot = cleaned[cleaned["Item"].eq("Hot Spice Apple Tea Cider")].iloc[0]
            self.assertEqual(int(hot["ice_pct"]), 0)
            self.assertIn("No Ice", str(hot["Modifiers Applied"]))


if __name__ == "__main__":
    unittest.main()

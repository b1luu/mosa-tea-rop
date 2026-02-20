import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "src" / "canonicalize.py"


def write_csv(path: Path, rows, columns):
    df = pd.DataFrame(rows, columns=columns)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


class DataQualityContractTests(unittest.TestCase):
    def run_contract_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            clean_path = tmp_path / "clean.csv"
            token_map_path = tmp_path / "modifier_token_map.csv"
            item_rules_path = tmp_path / "item_rules.csv"
            blend_rules_path = tmp_path / "item_blend_rules.csv"
            default_components_path = tmp_path / "item_default_component.csv"
            output_path = tmp_path / "out" / "canonicalized.csv"
            debug_output_path = tmp_path / "out" / "canonicalized_debug.csv"

            clean_rows = [
                [
                    "2026-01-01",
                    "Mosa Signature",
                    "Brown Sugar Mist",
                    1,
                    "Boba x2, 50% Ice, 50% Sugar",
                    50,
                    50,
                ],
                [
                    "2026-01-01",
                    "Fresh Fruit Tea",
                    "Fresh Lemon Tea",
                    1,
                    "Green Tea, 50% Ice, 50% Sugar",
                    50,
                    50,
                ],
                [
                    "2026-01-01",
                    "Fresh Fruit Tea",
                    "Fresh Mango Tea",
                    1,
                    "50% Ice, 50% Sugar",
                    50,
                    50,
                ],
                [
                    "2026-01-01",
                    "Fresh Fruit Tea",
                    "Fresh Orange Tea",
                    1,
                    "Green Tea, Four Seasons Tea, 50% Ice, 50% Sugar",
                    50,
                    50,
                ],
                [
                    "2026-01-01",
                    "Mosa Signature",
                    "Grapefruit Bloom",
                    1,
                    "50% Ice, 50% Sugar",
                    50,
                    50,
                ],
                [
                    "2026-01-01",
                    "Mosa Signature",
                    "TGY Special",
                    1,
                    "50% Ice, 50% Sugar",
                    50,
                    50,
                ],
                [
                    "2026-01-01",
                    "Hot Drink",
                    "Hot Signature Black Tea",
                    1,
                    "No Ice, No Sugar",
                    0,
                    0,
                ],
            ]
            token_rows = [
                ["Green Tea", "tea_base", "green_tea"],
                ["Four Seasons Tea", "tea_base", "four_seasons_tea"],
                ["Boba", "topping", "boba"],
            ]
            item_rule_rows = [
                ["mosa_signature", "brown_sugar_mist", "tie_guan_yin", 0],
                ["fresh_fruit_tea", "fresh_lemon_tea", "", 1],
                ["fresh_fruit_tea", "fresh_mango_tea", "", 1],
                ["fresh_fruit_tea", "fresh_orange_tea", "", 1],
                ["mosa_signature", "grapefruit_bloom", "", 0],
                ["mosa_signature", "tgy_special", "tie_guan_yin", 0],
                ["hot_drink", "hot_signature_black_tea", "black", 0],
            ]
            blend_rows = [
                ["mosa_signature", "grapefruit_bloom", "buckwheat_barley", 0.25],
                ["mosa_signature", "grapefruit_bloom", "four_seasons", 0.75],
            ]
            default_component_rows = [
                ["mosa_signature", "brown_sugar_mist", "brown_sugar_cream_foam", 1],
                ["mosa_signature", "brown_sugar_mist", "osmanthus_syrup_shot", 1],
                ["mosa_signature", "grapefruit_bloom", "tea_jelly", 1],
                ["mosa_signature", "tgy_special", "brown_sugar_hun_kue", 1],
            ]

            write_csv(
                clean_path,
                clean_rows,
                [
                    "Date",
                    "Category",
                    "Item",
                    "Qty",
                    "Modifiers Applied",
                    "ice_pct",
                    "sugar_pct",
                ],
            )
            write_csv(
                token_map_path,
                token_rows,
                ["raw_token", "token_type", "canonical_value"],
            )
            write_csv(
                item_rules_path,
                item_rule_rows,
                ["category_key", "item_key", "default_tea_base", "requires_tea_choice"],
            )
            write_csv(
                blend_rules_path,
                blend_rows,
                ["category_key", "item_key", "component_tea", "share"],
            )
            write_csv(
                default_components_path,
                default_component_rows,
                ["category_key", "item_key", "component_key", "qty"],
            )

            subprocess.run(
                [
                    "python3",
                    str(SCRIPT_PATH),
                    "--input",
                    str(clean_path),
                    "--token-map",
                    str(token_map_path),
                    "--item-rules",
                    str(item_rules_path),
                    "--blend-rules",
                    str(blend_rules_path),
                    "--default-components",
                    str(default_components_path),
                    "--output",
                    str(output_path),
                    "--debug-output",
                    str(debug_output_path),
                ],
                check=True,
                cwd=str(REPO_ROOT),
            )

            slim = pd.read_csv(output_path)
            debug = pd.read_csv(debug_output_path)
            return slim, debug

    def test_data_quality_contract(self):
        slim, debug = self.run_contract_fixture()

        self.assertEqual(len(slim), len(debug))
        self.assertFalse((slim["tea_resolution"] == "unknown").any())

        # rows requiring tea choice must resolve as override, missing_choice, or conflict
        req = debug[pd.to_numeric(debug["requires_tea_choice"], errors="coerce").fillna(0).eq(1)]
        allowed = {"override", "missing_choice", "conflict"}
        self.assertTrue(req["tea_resolution"].isin(allowed).all())

        # topping consistency invariants
        toppings_non_empty = slim["toppings_list"].fillna("").astype(str).str.strip().ne("")
        self.assertFalse(((~slim["has_topping"]) & toppings_non_empty).any())
        self.assertFalse((slim["has_topping"] & ~toppings_non_empty).any())
        self.assertFalse((slim["has_multiple_toppings"] & (slim["topping_types_count"] < 2)).any())
        self.assertFalse(((slim["topping_types_count"] == 0) & (slim["topping_units_total"] > 0)).any())

        # osmanthus syrup is explicitly excluded from topping outputs
        self.assertFalse(
            slim["toppings_list"].fillna("").astype(str).str.contains("osmanthus_syrup_shot").any()
        )
        self.assertFalse(
            slim["toppings_qty"].fillna("").astype(str).str.contains("osmanthus_syrup_shot").any()
        )

        # spot checks for contract fixture behavior
        brown_sugar = slim[slim["item_key"].eq("brown_sugar_mist")].iloc[0]
        self.assertEqual(brown_sugar["toppings_qty"], "boba:2|brown_sugar_cream_foam:1")
        self.assertEqual(brown_sugar["topping_multiplier_class"], "double")

        hot_black = slim[slim["item_key"].eq("hot_signature_black_tea")].iloc[0]
        self.assertFalse(bool(hot_black["has_topping"]))
        hot_black_toppings = "" if pd.isna(hot_black["toppings_list"]) else str(hot_black["toppings_list"]).strip()
        self.assertEqual(hot_black_toppings, "")


if __name__ == "__main__":
    unittest.main()

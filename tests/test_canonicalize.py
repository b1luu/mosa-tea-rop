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


class CanonicalizePipelineTests(unittest.TestCase):
    def run_pipeline(
        self,
        clean_rows,
        token_rows,
        item_rule_rows,
        blend_rows,
        default_component_rows=None,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            clean_path = tmp_path / "clean.csv"
            token_map_path = tmp_path / "modifier_token_map.csv"
            item_rules_path = tmp_path / "item_rules.csv"
            blend_rules_path = tmp_path / "item_blend_rules.csv"
            default_components_path = tmp_path / "item_default_component.csv"
            output_path = tmp_path / "out" / "canonicalized.csv"
            debug_output_path = tmp_path / "out" / "canonicalized_debug.csv"

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
                default_component_rows or [],
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

            return pd.read_csv(output_path), pd.read_csv(debug_output_path)

    def test_writes_two_outputs_with_expected_columns(self):
        clean_rows = [
            ["2026-01-01", "Mosa Signature", "TGY Special", 1, "50% Ice, 50% Sugar", 50, 50],
        ]
        token_rows = []
        item_rule_rows = [
            ["mosa_signature", "tgy_special", "tie_guan_yin", 0],
        ]
        blend_rows = []

        slim, debug = self.run_pipeline(clean_rows, token_rows, item_rule_rows, blend_rows)

        self.assertEqual(
            list(slim.columns),
            [
                "Date",
                "Category",
                "Item",
                "Qty",
                "Modifiers Applied",
                "ice_pct",
                "sugar_pct",
                "has_topping",
                "has_multiple_toppings",
                "toppings_list",
                "toppings_qty",
                "topping_types_count",
                "topping_units_total",
                "max_single_topping_qty",
                "topping_multiplier_class",
                "category_key",
                "item_key",
                "tea_base_final",
                "tea_resolution",
            ],
        )
        self.assertIn("tea_blend", debug.columns)
        self.assertIn("tea_base_override", debug.columns)
        self.assertIn("requires_tea_choice", debug.columns)

    def test_blend_default_applies_when_no_override(self):
        clean_rows = [
            [
                "2026-01-01",
                "Mosa Signature",
                "Grapefruit Bloom",
                1,
                "50% Ice, 50% Sugar",
                50,
                50,
            ],
        ]
        token_rows = []
        item_rule_rows = [
            ["mosa_signature", "grapefruit_bloom", "", 0],
        ]
        blend_rows = [
            ["mosa_signature", "grapefruit_bloom", "buckwheat_barley", 0.25],
            ["mosa_signature", "grapefruit_bloom", "four_seasons", 0.75],
        ]

        slim, _ = self.run_pipeline(clean_rows, token_rows, item_rule_rows, blend_rows)

        self.assertEqual(slim.loc[0, "tea_resolution"], "blend")
        self.assertEqual(slim.loc[0, "tea_base_final"], "buckwheat_barley:0.25|four_seasons:0.75")

    def test_override_beats_blend(self):
        clean_rows = [
            [
                "2026-01-01",
                "Mosa Signature",
                "Grapefruit Bloom",
                1,
                "Four Seasons Tea, 50% Ice, 50% Sugar",
                50,
                50,
            ],
        ]
        token_rows = [
            ["Four Seasons Tea", "tea_base", "four_seasons_tea"],
        ]
        item_rule_rows = [
            ["mosa_signature", "grapefruit_bloom", "", 0],
        ]
        blend_rows = [
            ["mosa_signature", "grapefruit_bloom", "buckwheat_barley", 0.25],
            ["mosa_signature", "grapefruit_bloom", "four_seasons", 0.75],
        ]

        slim, debug = self.run_pipeline(clean_rows, token_rows, item_rule_rows, blend_rows)

        self.assertEqual(slim.loc[0, "tea_resolution"], "override")
        self.assertEqual(slim.loc[0, "tea_base_final"], "four_seasons")
        self.assertEqual(debug.loc[0, "tea_blend"], "buckwheat_barley:0.25|four_seasons:0.75")

    def test_requires_choice_without_override_is_missing_choice(self):
        clean_rows = [
            ["2026-01-01", "Fresh Fruit Tea", "Fresh Mango Tea", 1, "50% Ice, 50% Sugar", 50, 50],
        ]
        token_rows = [
            ["Four Seasons Tea", "tea_base", "four_seasons_tea"],
            ["Green Tea", "tea_base", "green_tea"],
        ]
        item_rule_rows = [
            ["fresh_fruit_tea", "fresh_mango_tea", "", 1],
        ]
        blend_rows = []

        slim, _ = self.run_pipeline(clean_rows, token_rows, item_rule_rows, blend_rows)

        self.assertEqual(slim.loc[0, "tea_resolution"], "missing_choice")
        self.assertTrue(pd.isna(slim.loc[0, "tea_base_final"]))

    def test_two_tea_override_tokens_become_conflict(self):
        clean_rows = [
            [
                "2026-01-01",
                "Fresh Fruit Tea",
                "Fresh Lemon Tea",
                1,
                "Green Tea, Four Seasons Tea, 50% Ice, 50% Sugar",
                50,
                50,
            ],
        ]
        token_rows = [
            ["Four Seasons Tea", "tea_base", "four_seasons_tea"],
            ["Green Tea", "tea_base", "green_tea"],
        ]
        item_rule_rows = [
            ["fresh_fruit_tea", "fresh_lemon_tea", "", 1],
        ]
        blend_rows = []

        slim, debug = self.run_pipeline(clean_rows, token_rows, item_rule_rows, blend_rows)

        self.assertEqual(slim.loc[0, "tea_resolution"], "conflict")
        self.assertTrue(pd.isna(slim.loc[0, "tea_base_final"]))
        self.assertEqual(debug.loc[0, "tea_override_conflict"], "four_seasons|green")

    def test_toppings_are_aggregated_with_multiplier(self):
        clean_rows = [
            [
                "2026-01-01",
                "Mosa Signature",
                "TGY Special",
                1,
                "Boba x2, Lychee Jelly, 50% Ice, 50% Sugar",
                50,
                50,
            ],
        ]
        token_rows = [
            ["Boba", "topping", "boba"],
            ["Lychee Jelly", "topping", "lychee_jelly"],
        ]
        item_rule_rows = [
            ["mosa_signature", "tgy_special", "tie_guan_yin", 0],
        ]
        blend_rows = []

        slim, _ = self.run_pipeline(clean_rows, token_rows, item_rule_rows, blend_rows)

        self.assertTrue(bool(slim.loc[0, "has_topping"]))
        self.assertTrue(bool(slim.loc[0, "has_multiple_toppings"]))
        self.assertEqual(slim.loc[0, "toppings_list"], "boba|lychee_jelly")
        self.assertEqual(slim.loc[0, "toppings_qty"], "boba:2|lychee_jelly:1")
        self.assertEqual(int(slim.loc[0, "topping_types_count"]), 2)
        self.assertAlmostEqual(float(slim.loc[0, "topping_units_total"]), 3.0)
        self.assertAlmostEqual(float(slim.loc[0, "max_single_topping_qty"]), 2.0)
        self.assertEqual(slim.loc[0, "topping_multiplier_class"], "double")

    def test_mosa_signature_default_toppings_are_included_and_osmanthus_ignored(self):
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
        ]
        token_rows = [
            ["Boba", "topping", "boba"],
        ]
        item_rule_rows = [
            ["mosa_signature", "brown_sugar_mist", "tie_guan_yin", 0],
        ]
        blend_rows = []
        default_component_rows = [
            ["mosa_signature", "brown_sugar_mist", "brown_sugar_cream_foam", 1],
            ["mosa_signature", "brown_sugar_mist", "osmanthus_syrup_shot", 1],
        ]

        slim, _ = self.run_pipeline(
            clean_rows,
            token_rows,
            item_rule_rows,
            blend_rows,
            default_component_rows=default_component_rows,
        )

        self.assertTrue(bool(slim.loc[0, "has_topping"]))
        self.assertEqual(slim.loc[0, "toppings_list"], "boba|brown_sugar_cream_foam")
        self.assertEqual(slim.loc[0, "toppings_qty"], "boba:2|brown_sugar_cream_foam:1")
        self.assertEqual(int(slim.loc[0, "topping_types_count"]), 2)
        self.assertAlmostEqual(float(slim.loc[0, "topping_units_total"]), 3.0)
        self.assertEqual(slim.loc[0, "topping_multiplier_class"], "double")


if __name__ == "__main__":
    unittest.main()

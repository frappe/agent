from __future__ import annotations

import unittest
from pathlib import Path


class TestCfgParser(unittest.TestCase):
    def test_parse_fts_index_prefix_lengths_from_cfg(self):
        from agent.utils import parse_fts_index_prefixlen_from_cfg

        test_dir = Path(__file__).parent
        cfg_path = test_dir / "files" / "1_test_cfg_parser_file.cfg"

        result = parse_fts_index_prefixlen_from_cfg(cfg_path)

        self.assertEqual(len(result), 2)
        self.assertIn("ft_content", result)
        self.assertIn("ft_content2", result)
        self.assertEqual(result.get("ft_content"), 0)
        self.assertEqual(result.get("ft_content2"), 1)

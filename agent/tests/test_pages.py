from __future__ import annotations

import glob
import os
import unittest

PAGES_DIRECTORY = os.path.join(os.path.dirname(__file__), "..", "pages")
BENCH_NGINX_TEMPLATE = os.path.join(
    os.path.dirname(__file__), "..", "templates", "bench", "nginx.conf.jinja2"
)


def read_pages():
    for path in sorted(glob.glob(os.path.join(PAGES_DIRECTORY, "*.html"))):
        with open(path) as f:
            yield os.path.basename(path), f.read()


class TestPages(unittest.TestCase):
    """The error pages are served by nginx when the site is already broken."""

    def test_pages_are_self_contained(self):
        # an external font or stylesheet would hang on the very networks these pages appear on
        for name, html in read_pages():
            self.assertNotIn("<link", html, name)
            self.assertNotIn('src="http', html, name)

    def test_visited_links_are_marked_but_never_the_button(self):
        # a bare a:visited outranks .button's own colour and repaints its white label purple
        for name, html in read_pages():
            self.assertIn("a:not(.button):visited", html, name)
            self.assertNotIn(".button:visited", html, name)

    def test_placeholder_dashboard_links_are_rewritten(self):
        # the link ships pointing at the dashboard root; only the script knows the site name
        for name, html in read_pages():
            if "dashboard-url" in html:
                self.assertIn("dashboard/sites/${window.location.hostname}", html, name)

    def test_bench_name_placeholder_is_substituted_by_nginx(self):
        # an unsubstituted __BENCH_NAME__ ships a dead link to the site owner
        with open(BENCH_NGINX_TEMPLATE) as f:
            template = f.read()

        substituted = "sub_filter '__BENCH_NAME__' '{{ bench_name }}';"
        for name, html in read_pages():
            if "__BENCH_NAME__" not in html:
                continue
            page_locations = template.count(f"location = /{name} {{")
            self.assertTrue(page_locations, f"{name} is not served by the bench nginx config")
            self.assertEqual(template.count(substituted), page_locations, name)

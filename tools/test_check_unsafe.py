import unittest
from check_unsafe import unsafe_count


class UnsafeInventoryTests(unittest.TestCase):
    def test_counts_blocks_functions_and_impls(self):
        self.assertEqual(unsafe_count('unsafe { f() } unsafe fn x() {} unsafe impl Send for X {}'), 3)

    def test_ignores_comments_and_strings(self):
        self.assertEqual(unsafe_count('// unsafe {}\n/* outer /* unsafe {} */ done */\n"unsafe {}" r##"unsafe {}"## br#"unsafe {}"#'), 0)

    def test_keeps_lifetimes_and_char_quotes_from_hiding_code(self):
        self.assertEqual(unsafe_count("fn x<'a>() { let c = '\"'; unsafe { f() } }"), 1)

    def test_does_not_merge_keywords_across_comments(self):
        self.assertEqual(unsafe_count('un/* comment */safe unsafe_code'), 0)


if __name__ == '__main__':
    unittest.main()

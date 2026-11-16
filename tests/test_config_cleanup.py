import unittest
from unittest.mock import patch
from edzbot.config import Config


class CleanupConfigTests(unittest.TestCase):
    def test_auto_cleanup_default_off(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {'AUTO_CLEANUP': ''}, clear=True):
                with self.assertRaises(ValueError):
                    Config.load(require_telegram=False)
            with patch.dict('os.environ', {}, clear=True):
                self.assertFalse(Config.load(require_telegram=False).auto_cleanup)

    def test_auto_cleanup_opt_in(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {'AUTO_CLEANUP': 'true'}, clear=True):
                self.assertTrue(Config.load(require_telegram=False).auto_cleanup)


if __name__ == '__main__':
    unittest.main()

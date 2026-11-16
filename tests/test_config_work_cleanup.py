import unittest
from unittest.mock import patch
from edzbot.config import Config


class AutoWorkCleanupConfigTests(unittest.TestCase):
    def test_default_off(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {}, clear=True):
                self.assertFalse(Config.load(require_telegram=False).auto_work_cleanup)

    def test_explicit_opt_in(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {'AUTO_WORK_CLEANUP': 'true'}, clear=True):
                self.assertTrue(Config.load(require_telegram=False).auto_work_cleanup)


if __name__ == '__main__':
    unittest.main()

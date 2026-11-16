import unittest
from unittest.mock import patch
from edzbot.config import Config

class ConfigPlanTests(unittest.TestCase):
    def test_default_off(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {}, clear=True):
                c=Config.load(require_telegram=False)
                self.assertFalse(c.auto_plan_queue)
                self.assertEqual(20, c.plan_max_pending_artifacts)
                self.assertEqual(50*1024**2, c.plan_max_single_bytes)
                self.assertEqual("", c.plan_batch_id)
                self.assertEqual(1, c.plan_batch_max_sources)

    def test_optin_config(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {'AUTO_PLAN_QUEUE':'true','PLAN_MAX_PENDING_ARTIFACTS':'25','PLAN_MAX_SINGLE_MIB':'30'}, clear=True):
                c=Config.load(require_telegram=False)
                self.assertTrue(c.auto_plan_queue)
                self.assertEqual(25,c.plan_max_pending_artifacts)
                self.assertEqual(30*1024**2,c.plan_max_single_bytes)

    def test_invalid_config_blocks(self):
        with patch('edzbot.config.load_dotenv', return_value=None):
            with patch.dict('os.environ', {'PLAN_MAX_PENDING_ARTIFACTS':'0'}, clear=True):
                with self.assertRaises(ValueError):
                    Config.load(require_telegram=False)

if __name__=='__main__': unittest.main()

import unittest
from unittest.mock import patch

import bot_status_display as display


class BotStatusDisplayTests(unittest.TestCase):
    @patch("bot_status_display.socket.gethostname", return_value="kraken-host.example")
    def test_hostname_page(self, _gethostname):
        self.assertEqual(display.screen_lines(0), ["Hostname:", "kraken-host"])

    @patch("bot_status_display.ip_address_text", return_value="192.168.1.20")
    def test_ip_address_page(self, ip_address_text):
        self.assertEqual(display.screen_lines(1), ["IP Address:", "192.168.1.20"])
        ip_address_text.assert_called_once_with()

    @patch("bot_status_display.service_status_text", return_value="up")
    def test_service_status_page(self, service_status_text):
        self.assertEqual(display.screen_lines(2), ["Bot Status:", "up"])
        service_status_text.assert_called_once_with()

    def test_pages_wrap(self):
        with patch("bot_status_display.socket.gethostname", return_value="wrapped-host"):
            self.assertEqual(display.screen_lines(3), ["Hostname:", "wrapped-host"])

    def test_status_requires_all_configured_services_to_be_active(self):
        with patch("bot_status_display.getenv", return_value="one.service,two.service"):
            with patch("bot_status_display.service_state", side_effect=["active", "inactive"]):
                self.assertEqual(display.service_status_text(), "down")


if __name__ == "__main__":
    unittest.main()

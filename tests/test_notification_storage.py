import os
import unittest
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "postgresql://oms4:oms4@localhost:5432/oms4")

from app.notification_storage import NotificationStorage


class NotificationStorageIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = NotificationStorage()
        try:
            cls.storage.migrate()
        except Exception as exc:  # noqa: BLE001 - local integration dependency may be absent.
            raise unittest.SkipTest(f"PostgreSQL is unavailable: {exc}") from exc

    def test_template_versions_and_delivery_lifecycle(self) -> None:
        template_code = f"test.{uuid4().hex}"
        first = self.storage.upsert_active_template(template_code, "Subject 1", "Body 1")
        second = self.storage.upsert_active_template(template_code, "Subject 2", "Body 2")
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        self.assertTrue(second["active"])

        notification_id = str(uuid4())
        delivery, created = self.storage.create_delivery(
            notification_id,
            "user@example.com",
            second,
            {"to": "user@example.com", "subject": "Subject 2", "body": "Body 2"},
            "test-correlation",
        )
        self.assertTrue(created)
        self.assertEqual(delivery["status"], "queued")
        self.assertEqual(self.storage.update_delivery(notification_id, "retry", "temporary", 1)["status"], "retry")
        sent = self.storage.update_delivery(notification_id, "sent")
        self.assertEqual(sent["status"], "sent")
        self.assertIsNotNone(sent["sentAt"])


if __name__ == "__main__":
    unittest.main()

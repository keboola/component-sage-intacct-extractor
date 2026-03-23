import os
import unittest

import mock
from freezegun import freeze_time
from keboola.component.exceptions import UserException

from component import Component


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    @mock.patch("component.SageIntacctClient")
    def test_invalid_incremental_field_raises_user_exception(self, mock_client_class):
        """Incremental field not in object schema must raise UserException immediately."""
        mock_client = mock_client_class.return_value
        mock_client.get_object_fields.return_value = {"id": "string", "name": "string", "status": "string"}

        data_dir = os.path.join(os.path.dirname(__file__), "functional", "01_full_load_customers", "source", "data")
        with mock.patch.dict(os.environ, {"KBC_DATADIR": data_dir}):
            with mock.patch(
                "component.Configuration",
                return_value=mock.MagicMock(
                    source=mock.MagicMock(
                        endpoint="accounts-receivable/customer",
                        incremental_field="whencreated",
                        initial_since="",
                        columns=[],
                        locations=[],
                    ),
                    destination=mock.MagicMock(
                        incremental=True,
                        primary_key=["id"],
                        table_name="customers",
                    ),
                    authorization=mock.MagicMock(
                        client_id="dummy",
                        client_secret="dummy",
                        username="user@company",
                        entity=None,
                    ),
                    batch_size=100,
                ),
            ):
                comp = Component()
                comp.client = mock_client

                with self.assertRaises(UserException) as ctx:
                    comp.run()

        self.assertIn("whencreated", str(ctx.exception))
        self.assertIn("accounts-receivable/customer", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

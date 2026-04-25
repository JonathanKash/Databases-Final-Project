from unittest import TestCase, main
from datetime import date, timedelta
from importlib import reload

import db_handler as db
from MARIADB_CREDS import DB_CONFIG
from models.Item import Item
from models.Customer import Customer


# Fixed test IDs (16 chars each)
TEST_ITEM_ID     = "PUBTEST_ITEM0000"
TEST_CUSTOMER_ID = "PUBTEST_CUST0000"


class PublicTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = reload(db)

    @classmethod
    def tearDownClass(cls):
        try:
            cls._reset(cls)
            cls.db.cur.close()
            cls.db.conn.close()
        except Exception:
            pass

    def _reset(self):
        self.db.cur.execute("DELETE FROM waitlist")
        self.db.cur.execute("DELETE FROM rental")
        self.db.cur.execute("DELETE FROM rental_history WHERE item_id = ?", (TEST_ITEM_ID,))
        self.db.cur.execute("DELETE FROM item WHERE i_item_id = ?", (TEST_ITEM_ID,))
        self.db.cur.execute("DELETE FROM customer WHERE c_customer_id = ?", (TEST_CUSTOMER_ID,))
        self.db.conn.commit()

    def setUp(self):
        self._reset()

    # -------------------------------------------------------------------------
    # Static test data
    # -------------------------------------------------------------------------

    @staticmethod
    def get_item():
        return Item(
            item_id=TEST_ITEM_ID,
            product_name="Public Test Item",
            brand="PublicBrand",
            category="PublicCategory",
            manufact="PublicManufact",
            current_price=19.99,
            start_year=2021,
            num_owned=5,
        )

    @staticmethod
    def get_customer():
        return Customer(
            customer_id=TEST_CUSTOMER_ID,
            name="Public Tester",
            email="public.tester@test.com",
            address="5678 Test Ave, Gainesville, FL 32601",
        )

    def _insert_item(self):
        item = self.get_item()
        self.db.cur.execute(
            "INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, "
            "i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned) "
            "VALUES ((SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item AS tmp), "
            "?, ?, ?, ?, NULL, ?, ?, ?, ?)",
            (item.item_id, f"{item.start_year}-01-01", item.product_name,
             item.brand, item.category, item.manufact, item.current_price, item.num_owned)
        )
        self.db.conn.commit()
        return item

    def _insert_customer(self):
        customer = self.get_customer()
        self.db.cur.execute(
            "INSERT INTO customer_address "
            "(ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) "
            "VALUES ((SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address AS tmp), "
            "?, ?, ?, ?, ?)",
            ("5678", "Test Ave", "Gainesville", "FL", "32601")
        )
        self.db.cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
        addr_sk = self.db.cur.fetchone()[0]
        self.db.cur.execute(
            "INSERT INTO customer "
            "(c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) "
            "VALUES ((SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer AS tmp), "
            "?, ?, ?, ?, ?)",
            (customer.customer_id, "Public", "Tester", customer.email, addr_sk)
        )
        self.db.conn.commit()
        return customer

    # -------------------------------------------------------------------------
    # Tests
    # -------------------------------------------------------------------------

    def test_add_item(self):
        new_item = self.get_item()
        self.db.add_item(new_item=new_item)

        self.db.cur.execute(
            "SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact, "
            "i_current_price, YEAR(i_rec_start_date), i_num_owned "
            "FROM item WHERE i_item_id = ?",
            (new_item.item_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(new_item.item_id, row[0].strip())
        self.assertEqual(new_item.product_name, row[1].strip())
        self.assertEqual(new_item.num_owned, row[7])

    def test_add_customer(self):
        new_customer = self.get_customer()
        self.db.add_customer(new_customer=new_customer)

        self.db.cur.execute(
            "SELECT c_customer_id, TRIM(c_first_name), TRIM(c_last_name), TRIM(c_email_address) "
            "FROM customer WHERE c_customer_id = ?",
            (new_customer.customer_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(new_customer.customer_id, row[0].strip())
        self.assertEqual("Public", row[1])
        self.assertEqual("Tester", row[2])
        self.assertEqual(new_customer.email, row[3])

    def test_edit_customer(self):
        self._insert_customer()
        updated = Customer(
            customer_id="PUBTEST_EDIT0000",
            name="Edited Name",
            email="edited@test.com",
            address="9999 New Rd, Tampa, FL 33601",
        )

        self.db.edit_customer(original_customer_id=TEST_CUSTOMER_ID, new_customer=updated)

        # Old ID gone
        self.db.cur.execute(
            "SELECT c_customer_id FROM customer WHERE c_customer_id = ?", (TEST_CUSTOMER_ID,)
        )
        self.assertIsNone(self.db.cur.fetchone())

        # New ID present
        self.db.cur.execute(
            "SELECT c_customer_id, TRIM(c_email_address) FROM customer WHERE c_customer_id = ?",
            (updated.customer_id,)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(updated.customer_id, row[0].strip())
        self.assertEqual(updated.email, row[1])

        # Cleanup extra test customer
        self.db.cur.execute(
            "DELETE FROM customer WHERE c_customer_id = ?", (updated.customer_id,)
        )
        self.db.conn.commit()

    def test_rent_item(self):
        item = self._insert_item()
        customer = self._insert_customer()

        self.db.rent_item(item.item_id, customer.customer_id)

        self.db.cur.execute(
            "SELECT item_id, customer_id, rental_date, due_date FROM rental "
            "WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)

        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()

        self.assertEqual(item.item_id, row[0].strip())
        self.assertEqual(customer.customer_id, row[1].strip())
        self.assertEqual(today, str(row[2]))
        self.assertEqual(due, str(row[3]))

    def test_return_book(self):
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        due = (date.today() + timedelta(days=14)).isoformat()

        self.db.cur.execute(
            "INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)",
            (item.item_id, customer.customer_id, today, due)
        )
        self.db.conn.commit()

        self.db.return_item(item_id=item.item_id, customer_id=customer.customer_id)

        # Should be removed from rental
        self.db.cur.execute(
            "SELECT * FROM rental WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        self.assertIsNone(self.db.cur.fetchone())

        # Should appear in rental_history
        self.db.cur.execute(
            "SELECT return_date FROM rental_history WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(today, str(row[0]))

    def test_grant_extension(self):
        item = self._insert_item()
        customer = self._insert_customer()
        today = date.today().isoformat()
        original_due = (date.today() + timedelta(days=14)).isoformat()

        self.db.cur.execute(
            "INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)",
            (item.item_id, customer.customer_id, today, original_due)
        )
        self.db.conn.commit()

        self.db.grant_extension(item_id=item.item_id, customer_id=customer.customer_id)

        self.db.cur.execute(
            "SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        new_due = str(self.db.cur.fetchone()[0])
        expected_due = (date.today() + timedelta(days=28)).isoformat()
        self.assertEqual(expected_due, new_due)

    def test_waitlist_customer(self):
        item = self._insert_item()
        customer = self._insert_customer()

        place = self.db.waitlist_customer(item_id=item.item_id, customer_id=customer.customer_id)
        self.assertEqual(1, place)

        self.db.cur.execute(
            "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?",
            (item.item_id, customer.customer_id)
        )
        row = self.db.cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(1, row[0])

    def test_update_waitlist(self):
        item = self._insert_item()
        customer = self._insert_customer()

        # Insert 2 entries
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 1)
        )
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, "PLACEHOLDER_CUST", 2)  # 16 chars
        )
        self.db.conn.commit()

        self.db.update_waitlist(item_id=item.item_id)

        # Position 1 should be gone
        self.db.cur.execute(
            "SELECT customer_id FROM waitlist WHERE item_id = ?", (item.item_id,)
        )
        remaining = [row[0].strip() for row in self.db.cur.fetchall()]
        self.assertNotIn(customer.customer_id, remaining)
        self.assertIn("PLACEHOLDER_CUST", remaining)

        # Remaining entry should now be at position 1
        self.db.cur.execute(
            "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?",
            (item.item_id, "PLACEHOLDER_CUST")
        )
        self.assertEqual(1, self.db.cur.fetchone()[0])

    def test_get_filtered_items(self):
        item = self._insert_item()

        results = self.db.get_filtered_items(
            filter_attributes=Item(item_id=item.item_id),
            use_patterns=False
        )
        self.assertEqual(1, len(results))
        self.assertEqual(item.item_id, results[0].item_id)
        self.assertEqual(item.product_name, results[0].product_name)
        self.assertEqual(item.num_owned, results[0].num_owned)

    def test_get_filtered_items_patterns(self):
        item = self._insert_item()

        results = self.db.get_filtered_items(
            filter_attributes=Item(product_name="Public%Item"),
            use_patterns=True
        )
        actual_ids = [r.item_id for r in results]
        self.assertIn(item.item_id, actual_ids)

    def test_get_filtered_customers(self):
        customer = self._insert_customer()

        results = self.db.get_filtered_customers(
            filter_attributes=Customer(customer_id=customer.customer_id)
        )
        self.assertEqual(1, len(results))
        self.assertEqual(customer.customer_id, results[0].customer_id)
        self.assertEqual(customer.email, results[0].email)

    def test_get_filtered_customers_patterns(self):
        customer = self._insert_customer()

        results = self.db.get_filtered_customers(
            filter_attributes=Customer(email="public.tester%"),
            use_patterns=True
        )
        actual_ids = [r.customer_id for r in results]
        self.assertIn(customer.customer_id, actual_ids)

    def test_number_in_stock(self):
        item = self._insert_item()

        # No rentals — should equal num_owned
        result = self.db.number_in_stock(item.item_id)
        self.assertEqual(item.num_owned, result)

    def test_place_in_line(self):
        item = self._insert_item()
        customer = self._insert_customer()

        # Not on waitlist
        self.assertEqual(-1, self.db.place_in_line(item.item_id, customer.customer_id))

        # Add to waitlist
        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 1)
        )
        self.db.conn.commit()

        self.assertEqual(1, self.db.place_in_line(item.item_id, customer.customer_id))

    def test_line_length(self):
        item = self._insert_item()
        customer = self._insert_customer()

        self.assertEqual(0, self.db.line_length(item.item_id))

        self.db.cur.execute(
            "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
            (item.item_id, customer.customer_id, 1)
        )
        self.db.conn.commit()

        self.assertEqual(1, self.db.line_length(item.item_id))

    def test_save_changes(self):
        self.db.cur.execute(
            "INSERT INTO customer (c_customer_sk, c_customer_id) "
            "VALUES ((SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer AS tmp), ?)",
            (TEST_CUSTOMER_ID,)
        )
        self.db.save_changes()
        self.db.cur.close()
        self.db.conn.close()
        self.db = reload(db)

        self.db.cur.execute(
            "SELECT c_customer_id FROM customer WHERE c_customer_id = ?", (TEST_CUSTOMER_ID,)
        )
        result = self.db.cur.fetchone()
        self.assertEqual(TEST_CUSTOMER_ID, result[0].strip())

    def test_close_connection(self):
        from mariadb import connect
        conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"],
                       host=DB_CONFIG["host"], database=DB_CONFIG["database"],
                       port=DB_CONFIG["port"])
        cur = conn.cursor()

        cur.execute("SHOW PROCESSLIST")
        count_before = len(cur.fetchall())

        self.db.close_connection()

        cur.execute("SHOW PROCESSLIST")
        count_after = len(cur.fetchall())

        self.assertEqual(count_before - 1, count_after)
        self.db = reload(db)


if __name__ == '__main__':
    main()

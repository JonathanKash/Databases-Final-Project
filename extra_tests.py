from unittest import TestCase, main
from datetime import date, timedelta
from importlib import reload

import db_handler as db
from models.Item import Item
from models.Customer import Customer
from models.Rental import Rental
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist


ITEM1 = "EDGE_ITEM_000001"
ITEM2 = "EDGE_ITEM_000002"
CUST1 = "EDGE_CUST_000001"
CUST2 = "EDGE_CUST_000002"


class ExtraTests(TestCase):

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
		self.db.cur.execute("DELETE FROM waitlist WHERE item_id IN (?, ?)", (ITEM1, ITEM2))
		self.db.cur.execute("DELETE FROM rental WHERE item_id IN (?, ?)", (ITEM1, ITEM2))
		self.db.cur.execute("DELETE FROM rental_history WHERE item_id IN (?, ?)", (ITEM1, ITEM2))
		self.db.cur.execute("DELETE FROM item WHERE i_item_id IN (?, ?)", (ITEM1, ITEM2))
		self.db.cur.execute("DELETE FROM customer WHERE c_customer_id IN (?, ?)", (CUST1, CUST2))
		self.db.conn.commit()

	def setUp(self):
		self._reset()

	def add_item(self, item_id=ITEM1, price=25.00, year=2020, owned=2):
		item = Item(
			item_id=item_id,
			product_name="Edge Test Item",
			brand="EdgeBrand",
			category="EdgeCategory",
			manufact="EdgeManufact",
			current_price=price,
			start_year=year,
			num_owned=owned
		)
		self.db.add_item(item)
		self.db.save_changes()
		return item

	def add_customer(self, customer_id=CUST1, name="Edge Tester"):
		customer = Customer(
			customer_id=customer_id,
			name=name,
			email=f"{customer_id.lower()}@test.com",
			address="123 Edge St, Gainesville, FL 32601"
		)
		self.db.add_customer(customer)
		self.db.save_changes()
		return customer

	def test_number_in_stock_for_missing_item_returns_negative_one(self):
		self.assertEqual(-1, self.db.number_in_stock("DOES_NOT_EXIST"))

	def test_number_in_stock_decreases_after_rental(self):
		item = self.add_item(owned=2)
		customer = self.add_customer()

		self.assertEqual(2, self.db.number_in_stock(item.item_id))

		self.db.rent_item(item.item_id, customer.customer_id)
		self.db.save_changes()

		self.assertEqual(1, self.db.number_in_stock(item.item_id))

	def test_return_item_removes_active_rental(self):
		item = self.add_item()
		customer = self.add_customer()

		self.db.rent_item(item.item_id, customer.customer_id)
		self.db.save_changes()

		self.db.return_item(item.item_id, customer.customer_id)
		self.db.save_changes()

		active = self.db.get_filtered_rentals(Rental(item_id=item.item_id, customer_id=customer.customer_id))
		history = self.db.get_filtered_rental_histories(RentalHistory(item_id=item.item_id, customer_id=customer.customer_id))

		self.assertEqual(0, len(active))
		self.assertEqual(1, len(history))

	def test_waitlist_positions_shift_correctly(self):
		item = self.add_item()
		customer1 = self.add_customer(CUST1, "First Customer")
		customer2 = self.add_customer(CUST2, "Second Customer")

		self.assertEqual(1, self.db.waitlist_customer(item.item_id, customer1.customer_id))
		self.assertEqual(2, self.db.waitlist_customer(item.item_id, customer2.customer_id))
		self.db.save_changes()

		self.db.update_waitlist(item.item_id)
		self.db.save_changes()

		self.assertEqual(-1, self.db.place_in_line(item.item_id, customer1.customer_id))
		self.assertEqual(1, self.db.place_in_line(item.item_id, customer2.customer_id))

	def test_item_price_range_filter(self):
		self.add_item(ITEM1, price=10.00)
		self.add_item(ITEM2, price=50.00)

		results = self.db.get_filtered_items(
			filter_attributes=Item(),
			min_price=20.00,
			max_price=60.00
		)

		ids = [item.item_id for item in results]
		self.assertNotIn(ITEM1, ids)
		self.assertIn(ITEM2, ids)

	def test_item_year_range_filter(self):
		self.add_item(ITEM1, year=2018)
		self.add_item(ITEM2, year=2024)

		results = self.db.get_filtered_items(
			filter_attributes=Item(),
			min_start_year=2020,
			max_start_year=2025
		)

		ids = [item.item_id for item in results]
		self.assertNotIn(ITEM1, ids)
		self.assertIn(ITEM2, ids)

	def test_customer_name_pattern_filter(self):
		customer = self.add_customer(CUST1, "Maria Johnson")

		results = self.db.get_filtered_customers(
			filter_attributes=Customer(name="%Johnson"),
			use_patterns=True
		)

		ids = [c.customer_id for c in results]
		self.assertIn(customer.customer_id, ids)

	def test_customer_address_pattern_filter(self):
		customer = self.add_customer()

		results = self.db.get_filtered_customers(
			filter_attributes=Customer(address="%Gainesville%"),
			use_patterns=True
		)

		ids = [c.customer_id for c in results]
		self.assertIn(customer.customer_id, ids)

	def test_rental_date_range_filter(self):
		item = self.add_item()
		customer = self.add_customer()

		self.db.rent_item(item.item_id, customer.customer_id)
		self.db.save_changes()

		today = date.today().isoformat()

		results = self.db.get_filtered_rentals(
			filter_attributes=Rental(),
			min_rental_date=today,
			max_rental_date=today
		)

		ids = [(r.item_id, r.customer_id) for r in results]
		self.assertIn((item.item_id, customer.customer_id), ids)

	def test_waitlist_max_place_filter(self):
		item = self.add_item()
		customer1 = self.add_customer(CUST1, "First Customer")
		customer2 = self.add_customer(CUST2, "Second Customer")

		self.db.waitlist_customer(item.item_id, customer1.customer_id)
		self.db.waitlist_customer(item.item_id, customer2.customer_id)
		self.db.save_changes()

		results = self.db.get_filtered_waitlist(
			filter_attributes=Waitlist(item_id=item.item_id),
			max_place_in_line=1
		)

		self.assertEqual(1, len(results))
		self.assertEqual(customer1.customer_id, results[0].customer_id)


if __name__ == "__main__":
	main()
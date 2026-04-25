from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()

#helper functions
def split_name(name):
    if not name:
        return None, None
    name_parts = name.split(" ", 1)
    return name_parts[0], name_parts[1] if len(name_parts) > 1 else ""

def parse_address(address):
    street, city, statezip = address.split(", ")
    number, *street_name = street.split(" ")
    street_name = " ".join(street_name)
    state, zip_code = statezip.split(" ")
    return number, street_name, city, state, zip_code

def clean(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """

    cur.execute("SELECT MAX(i_item_sk) FROM item")
    max_sk = cur.fetchone()[0] or 0
    cur.execute("INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (max_sk+1, new_item.item_id, f"{new_item.start_year}-01-01", new_item.product_name, new_item.brand, new_item.category, new_item.category,
                    new_item.manufact, new_item.current_price, new_item.num_owned))

def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """

    first, last= split_name(new_customer.name)
    number, street, city, state, zip_code = parse_address(new_customer.address)

    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    addr_sk = (cur.fetchone()[0] or 0) +1
    cur.execute("INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) VALUES (?, ?, ?, ?, ?, ?)",
                (addr_sk, number, street, city, state, zip_code))
    
    cur.execute("SELECT MAX(c_customer_sk) FROM customer")
    cust_sk = (cur.fetchone()[0] or 0) +1
    cur.execute("INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) VALUES (?, ?, ?, ?, ?, ?)",
                (cust_sk, new_customer.customer_id, first, last, new_customer.email, addr_sk))

def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    if new_customer.name:
        first, last= split_name(new_customer.name)
        cur.execute("UPDATE customer SET c_first_name = ?, c_last_name = ? WHERE c_customer_id = ?", (first, last, original_customer_id))

    if new_customer.email:
        cur.execute("UPDATE customer SET c_email_address = ? WHERE c_customer_id = ?", (new_customer.email, original_customer_id))

    if new_customer.address:
        number, street, city, state, zip_code = parse_address(new_customer.address)
        cur.execute("UPDATE customer_address SET ca_street_number = ?, ca_street_name = ?, ca_city = ?, ca_state = ?, ca_zip = ? WHERE ca_address_sk = (SELECT c_current_addr_sk FROM customer WHERE c_customer_id = ?)",
                    (number, street, city, state, zip_code, original_customer_id))
    
    if new_customer.customer_id:
        cur.execute("UPDATE customer SET c_customer_id = ? WHERE c_customer_id = ?", (new_customer.customer_id, original_customer_id))


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """

    today = date.today()
    due_date = today + timedelta(days=14)

    cur.execute ("INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)", (item_id, customer_id, today, due_date))


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """

    pos = line_length(item_id) + 1
    cur.execute("INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)", (item_id, customer_id, pos))
    return pos

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """

    cur.execute("DELETE FROM waitlist WHERE item_id = ? AND place_in_line = 1", (item_id,))
    cur.execute("UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id = ?", (item_id,))


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """

    today= date.today()
    cur.execute("SELECT rental_date, due_date FROM rental WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    row = cur.fetchone()
    cur.execute("INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) VALUES (?, ?, ?, ?, ?)", (item_id, customer_id, row[0], row[1], today))
    cur.execute("DELETE FROM rental WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))

def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """

    cur.execute("UPDATE rental SET due_date = due_date + INTERVAL 14 DAY WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """

    query = "SELECT i_item_id, i_product_name, i_brand, i_category, i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned FROM item WHERE 1=1"
    params = []

    def add_filter(attribute, column):
        if attribute is not None:
            query_part = "LIKE ?" if use_patterns else "= ?"
            return f" AND {column} {query_part}", attribute
        return "", None
    
    for attr, col in [(filter_attributes.item_id, "i_item_id"), (filter_attributes.product_name, "i_product_name"), (filter_attributes.brand, "i_brand"), (filter_attributes.category, "i_category"), (filter_attributes.manufact, "i_manufact")]:
        q, p = add_filter(attr, col)
        query += q
        if p is not None:
            params.append(p)

    if min_price != -1:
        query += " AND i_current_price >= ?"
        params.append(min_price)

    if max_price != -1:
        query += " AND i_current_price <= ?"
        params.append(max_price)

    if min_start_year != -1:
        query += " AND YEAR(i_rec_start_date) >= ?"
        params.append(min_start_year)

    if max_start_year != -1:
        query += " AND YEAR(i_rec_start_date) <= ?"
        params.append(max_start_year)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    return [Item(clean(row[0]), clean(row[1]), clean(row[2]), clean(row[3]), clean(row[4]), row[5], row[6], row[7]) for row in rows]



def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """

    query= "SELECT c.c_customer_id, CONCAT(TRIM(c_first_name), ' ', TRIM(c_last_name)), c_email_address, CONCAT(TRIM(ca_street_number), ' ', TRIM(ca_street_name), ', ', TRIM(ca_city), ', ', TRIM(ca_state), ' ', TRIM(ca_zip)) FROM customer c JOIN customer_address a ON c.c_current_addr_sk = a.ca_address_sk WHERE 1=1"
    params = []

    if filter_attributes.customer_id is not None:
        query += " AND c.c_customer_id = ?"
        params.append(filter_attributes.customer_id)

    if filter_attributes.name is not None:
        query += " AND CONCAT(TRIM(c_first_name), ' ', TRIM(c_last_name)) " 
        query += "LIKE ?" if use_patterns else "= ?"
        params.append(filter_attributes.name)

    if filter_attributes.address is not None:
        query += " AND CONCAT(TRIM(ca_street_number), ' ', TRIM(ca_street_name), ', ', TRIM(ca_city), ', ', TRIM(ca_state), ' ', TRIM(ca_zip)) "
        query += "LIKE ?" if use_patterns else "= ?"
        params.append(filter_attributes.address)

    if filter_attributes.email is not None:
        query += " AND c_email_address " + ("LIKE ?" if use_patterns else "= ?")
        params.append(filter_attributes.email)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    return [Customer(clean(row[0]), clean(row[1]), clean(row[3]), clean(row[2])) for row in rows]

    

def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """

    query = "SELECT item_id, customer_id, rental_date, due_date FROM rental WHERE 1=1"
    params = []

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)

    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)

    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)

    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)

    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)

    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)
    
    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    return [Rental(clean(row[0]), clean(row[1]), str(row[2]), str(row[3])) for row in rows]



def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """

    query= "SELECT item_id, customer_id, rental_date, due_date, return_date FROM rental_history WHERE 1=1"
    params = []

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)

    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)

    if min_rental_date is not None:
        query += " AND rental_date >= ?"
        params.append(min_rental_date)

    if max_rental_date is not None:
        query += " AND rental_date <= ?"
        params.append(max_rental_date)

    if min_due_date is not None:
        query += " AND due_date >= ?"
        params.append(min_due_date)

    if max_due_date is not None:
        query += " AND due_date <= ?"
        params.append(max_due_date)

    if min_return_date is not None:
        query += " AND return_date >= ?"
        params.append(min_return_date)

    if max_return_date is not None:
        query += " AND return_date <= ?"
        params.append(max_return_date)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    return [RentalHistory(clean(row[0]), clean(row[1]), str(row[2]), str(row[3]), str(row[4])) for row in rows]  
    

def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """

    query = "SELECT item_id, customer_id, place_in_line FROM waitlist WHERE 1=1"
    params = []

    if filter_attributes.item_id is not None:
        query += " AND item_id = ?"
        params.append(filter_attributes.item_id)

    if filter_attributes.customer_id is not None:
        query += " AND customer_id = ?"
        params.append(filter_attributes.customer_id)

    if min_place_in_line != -1:
        query += " AND place_in_line >= ?"
        params.append(min_place_in_line)
    
    if max_place_in_line != -1:
        query += " AND place_in_line <= ?"
        params.append(max_place_in_line)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    return [Waitlist(clean(row[0]), clean(row[1]), row[2]) for row in rows]
    

def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """

    cur.execute("SELECT i_num_owned FROM item WHERE i_item_id = ?", (item_id,))
    row = cur.fetchone()
    if not row:
        return -1
    num_owned = row[0]
    cur.execute("SELECT COUNT(*) FROM rental WHERE item_id = ?", (item_id,))
    active_rentals = cur.fetchone()[0]
    return num_owned - active_rentals


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """

    cur.execute("SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?", (item_id, customer_id))
    row = cur.fetchone()
    return row[0] if row else -1



def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """

    cur.execute("SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item_id,))
    return cur.fetchone()[0]



def save_changes():
    """
    Commits all changes made to the db.
    """

    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()


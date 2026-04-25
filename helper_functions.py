import db_handler as db
from models.RentalHistory import RentalHistory
from datetime import timedelta, datetime
from models.Waitlist import Waitlist
from models.Item import Item
from models.Customer import Customer
from models.Rental import Rental

# Basic lists to make menu printing modular
MAIN_MENU_OPTIONS = [
    "Rent an Item",
    "Return an Item",
    "Grant an Extension",
    "Search a Table",
    "Add an Item",
    "Add a Customer",
    "Edit a Customer",
    "Exit"
]

TABLE_OPTIONS = [
    "Item",
    "Customer",
    "Rental",
    "Rental History",
    "Waitlist",
    "Cancel"
]

# These are used to filter attributes when searching through tables
ITEM_OPTIONS = [
    "Item ID",
    "Product Name",
    "Brand",
    "Manufacturer",
    "Category",
    "Min Price",
    "Max Price",
    "Min Start Year",
    "Max Start Year",
    "Continue",
    "Cancel"
]

CUSTOMER_OPTIONS = [
    "Customer ID",
    "Name",
    "Address",
    "Email",
    "Continue",
    "Cancel"
]

RENTAL_OPTIONS = [
    "Item ID",
    "Customer ID",
    "Min Rental Date",
    "Max Rental Date",
    "Min Due Date",
    "Max Due Date",
    "Continue",
    "Cancel"
]

WAITLIST_OPTIONS = [
    "Item ID",
    "Customer ID",
    "Min Place in line",
    "Max Place in line",
    "Continue",
    "Cancel"
]

RENTAL_HISTORY_OPTIONS = [
    "Item ID",
    "Customer ID",
    "Min Rental Date",
    "Max Rental Date",
    "Min Due Date",
    "Max Due Date",
    "Min Return Date",
    "Max Return Date",
    "Continue",
    "Cancel"
]

EDIT_CUSTOMER_OPTIONS = [
    "Customer ID",
    "Name",
    "Address",
    "Email",
    "Save",
    "Cancel"
]


# Given a generic list of objects, print them out. The object_name var helps it sound more specific
def print_list_of_objects(objects: list, object_name: str):
    if len(objects) == 0:
        print(f"No {object_name}s found")

    else:
        for o in objects:
            print("-" * 20)
            print(str(o)[:-1])
            print("-" * 20)

        print()
        print(f"Found {str(len(objects))} {object_name}{'s' if len(objects) > 1 else ''}.")


# Generic print menu function
def print_menu(menu_header, options):
    print(menu_header)

    for i, option in enumerate(options):
        print(f"{i + 1}. {option}")

    print()
    choice = input("Choice: ")
    print()

    return choice

# Wrapper functions to simplify menu printing and help code readability
def print_main_menu():
    menu_header = "What would you like to do?"
    return print_menu(menu_header, MAIN_MENU_OPTIONS)


def print_filter_menu(options):
    menu_header = "Which attribute would you like to filter?"
    return print_menu(menu_header, options)


def print_filter_item_menu():
    return print_filter_menu(ITEM_OPTIONS)


def print_filter_customer_menu():
    return print_filter_menu(CUSTOMER_OPTIONS)


def print_filter_waitlist_menu():
    return print_filter_menu(WAITLIST_OPTIONS)


def print_filter_rental_menu():
    return print_filter_menu(RENTAL_OPTIONS)


def print_filter_rental_history_menu():
    return print_filter_menu(RENTAL_HISTORY_OPTIONS)


def print_edit_customer_menu():
    menu_header = "Which attribute would you like to edit?"
    return print_menu(menu_header, EDIT_CUSTOMER_OPTIONS)


def handle_customer_menu_choice(choice, new_customer=Customer()):
    if choice == "1":
        new_customer_id = input("Customer ID: ")
        new_customer.customer_id = new_customer_id
    elif choice == "2":
        new_name = input("Name: ")
        new_customer.name = new_name
    elif choice == "3":
        new_address = input("Address: ")
        new_customer.address = new_address
    elif choice == "4":
        new_email = input("Email: ")
        new_customer.email = new_email
    elif choice not in ["5", "6"]: # Save and Cancel options
        print("Invalid choice")

    if choice != "6":
        print("Current Attributes:")
        print("--------------------")
        print(new_customer, end="")
        print("--------------------")
        print()

    return new_customer


def check_if_customer_exists(customer_id):
    customer_exists = len(db.get_filtered_customers(Customer(customer_id=customer_id))) == 1

    return customer_exists


def check_if_item_exists(item_id):
    item_exists = len(db.get_filtered_items(Item(item_id=item_id))) == 1

    return item_exists


def check_if_item_and_customer_exists(item_id, customer_id):
    item_exists = check_if_item_exists(item_id)
    customer_exists = check_if_customer_exists(customer_id)
    checks_passed = item_exists and customer_exists

    if not item_exists:
        print("Item not found")

    if not customer_exists:
        print("Customer not found")

    return checks_passed


def add_item():
    num_owned = 0
    start_year = -1
    current_price = -1
    item_id = input("Enter Item ID: ")
    item_exists = check_if_item_exists(item_id)

    if item_exists:
        print("An item with that ID already exists.")
        return

    product_name = input("Enter Product Name: ")
    brand = input("Enter Brand: ")
    category = input("Enter Category: ")
    manufact = input("Enter Manufacturer: ")

    while current_price == -1:
        try:
            current_price = float(input("Enter Current Price: "))

            if current_price < 0:
                raise ValueError("Price cannot be negative")

        except ValueError:
            print("Please enter a valid price")
            current_price = -1

    while start_year == -1:
        try:
            start_year = int(input("Enter Start Year: "))

            if start_year < 0:
                raise ValueError("Start Year cannot be negative")

        except ValueError:
            print("Please enter a valid start year")
            start_year = -1

    while num_owned == 0:
        try:
            num_owned = int(input("Enter the number of copies owned: "))

            if num_owned < 1:
                raise ValueError("Number of copies owned cannot be less than one")

        except ValueError:
            print("Please enter a valid number")
            num_owned = 0

    new_item = Item(item_id=item_id, product_name=product_name, brand=brand, category=category,
                    manufact=manufact, current_price=current_price, start_year=start_year, num_owned=num_owned)

    db.add_item(new_item)


def add_customer():
    customer_id = input("Enter Customer ID: ")
    customer_exists = check_if_customer_exists(customer_id)

    if customer_exists:
        print("A customer with that ID already exists.")
        return

    first_name = input("Enter First Name: ")
    last_name = input("Enter Last Name: ")
    email = input("Enter Email: ")
    street_number = input("Enter Street Number: ")
    street_name = input("Enter Street Name: ")
    city = input("Enter City: ")
    state = input("Enter State: ")
    zip_code = input("Enter Zip: ")

    name = f"{first_name} {last_name}"
    address = f"{street_number} {street_name}, {city}, {state} {zip_code}"

    new_customer = Customer(customer_id=customer_id, name=name, email=email, address=address)
    db.add_customer(new_customer=new_customer)


def edit_customer():
    og_customer_id = input("Customer's Customer ID: ")
    customer_exists = check_if_customer_exists(og_customer_id)

    if not customer_exists:
        print("No customer with that ID exists.")
        return

    new_customer = Customer()
    choice = '1'

    print()
    while choice != "5" and choice != "6":
        choice = print_edit_customer_menu()
        new_customer = handle_customer_menu_choice(choice, new_customer)

    if choice == "5":
        db.edit_customer(original_customer_id=og_customer_id, new_customer=new_customer)


def waitlist_customer(item_id=None, customer_id=None):
    if not check_if_item_and_customer_exists(item_id, customer_id):
        return

    if db.place_in_line(item_id=item_id, customer_id=customer_id) != -1:
        print("Customer is already waitlisted")
        return

    waitlist = input("Would you like to waitlist the Customer (Y/N): ").upper() == "Y"

    if waitlist:
        place_in_line = db.waitlist_customer(item_id=item_id, customer_id=customer_id)

        # Ordinal suffix logic
        last_digit = place_in_line % 10
        last_two_digits = place_in_line % 100
        num_suffix = "th"
        if last_digit == 1 and last_two_digits != 11:
            num_suffix = "st"
        elif last_digit == 2 and last_two_digits != 12:
            num_suffix = "nd"
        elif last_digit == 3 and last_two_digits != 13:
            num_suffix = "rd"

        print(f"The customer is now {place_in_line}{num_suffix} in line to rent the item")

    else:
        print("The customer was not waitlisted")


def rent_item():
    item_id = input("Enter Item ID: ")
    customer_id = input("Enter Customer ID: ")

    if not check_if_item_and_customer_exists(item_id, customer_id):
        return

    num_in_stock = db.number_in_stock(item_id=item_id)
    customer_has_item = len(db.get_filtered_rentals(Rental(item_id=item_id, customer_id=customer_id))) > 0
    customer_place_in_line = db.place_in_line(item_id=item_id, customer_id=customer_id)

    if customer_has_item:
        print("The customer has already rented this item")

    elif num_in_stock == 0:  # Out of stock, waitlist the customer
        if customer_place_in_line == -1:
            print("This item is not available right now.")
            waitlist_customer(item_id=item_id, customer_id=customer_id)
        else:
            print("The customer is waitlisted, but the item is still not available")

    else:  # Check if customer is able to rent the item
        people_in_line = db.line_length(item_id=item_id)

        if customer_place_in_line == 1 or people_in_line == 0:  # Customer is either next in line or there is no waitlist
            db.rent_item(item_id=item_id, customer_id=customer_id)
            db.update_waitlist(item_id=item_id)
            print("Successfully rented item")

        else:  # There is a waitlist and customer isn't next
            if people_in_line > 0:
                print("The customer is not next in line to rent this item.")

            if customer_place_in_line == -1:  # If the customer isn't waitlisted then ask to waitlist them
                print("The customer is not waitlisted for this item.")
                waitlist_customer(item_id=item_id, customer_id=customer_id)


def return_item():
    item_id = input("Enter Item ID: ")
    customer_id = input("Enter Customer ID: ")

    if not check_if_item_and_customer_exists(item_id, customer_id):
        return

    customer_has_item = len(db.get_filtered_rentals(Rental(item_id=item_id, customer_id=customer_id))) > 0

    if not customer_has_item:
        print("The customer does not have this item")

    else:
        db.return_item(item_id=item_id, customer_id=customer_id)
        print("Successfully returned the item")


def grant_extension():
    item_id = input("Enter Item ID: ")
    customer_id = input("Enter Customer ID: ")

    if not check_if_item_and_customer_exists(item_id, customer_id):
        return

    current_rental = db.get_filtered_rentals(Rental(item_id=item_id, customer_id=customer_id))

    if len(current_rental) == 0:
        print("The customer does not have this item")

    else:
        rental = current_rental[0]

        customer_already_has_extension = datetime.fromisoformat(rental.due_date) - datetime.fromisoformat(rental.rental_date) == timedelta(weeks=4)

        if customer_already_has_extension:
            print("The customer already has an extension and may not be granted another one")
        else:
            db.grant_extension(item_id=item_id, customer_id=customer_id)
            print("Successfully granted extension")


def search_items():
    use_patterns = input("Would you like to use patterns to search String attributes? (Y/N): ").upper() == "Y"
    new_item = Item()  # Create an empty item to hold filter attributes
    min_price = -1
    max_price = -1
    min_start_year = -1
    max_start_year = -1
    choice = "1"

    while choice != "10" and choice != "11":
        choice = print_filter_item_menu()
        try:
            if choice == "1":
                new_item.item_id = input("Item ID: ")
            elif choice == "2":
                new_item.product_name = input("Product Name: ")
            elif choice == "3":
                new_item.brand = input("Brand: ")
            elif choice == "4":
                new_item.manufact = input("Manufacturer: ")
            elif choice == "5":
                new_item.category = input("Category: ")
            elif choice == "6":
                min_price = float(input("Min Price: "))
            elif choice == "7":
                max_price = float(input("Max Price: "))
            elif choice == "8":
                min_start_year = int(input("Min Start Year: "))
            elif choice == "9":
                max_start_year = int(input("Max Start Year: "))
            elif choice not in ["10", "11"]:
                print("Unrecognized choice")

        except ValueError:
            print("Please enter a valid value")
            print()

        if choice == "11":
            return

        print()
        print("Current Filters:")
        print("--------------------")
        print(new_item, end="")
        if min_price != -1:
            print(f"Min Price: {min_price}")
        if max_price != -1:
            print(f"Max Price: {max_price}")
        if min_start_year != -1:
            print(f"Min Start Year: {min_start_year}")
        if max_start_year != -1:
            print(f"Max Start Year: {max_start_year}")
        print("--------------------")
        print()

    items = db.get_filtered_items(filter_attributes=new_item, use_patterns=use_patterns,
                                  min_price=min_price, max_price=max_price,
                                  min_start_year=min_start_year, max_start_year=max_start_year)
    print_list_of_objects(items, "item")


def search_customers():
    use_patterns = input("Would you like to use patterns to search String attributes? (Y/N): ").upper() == "Y"
    new_customer = Customer()
    choice = "1"

    while choice != "5" and choice != "6":
        choice = print_filter_customer_menu()

        if choice == "1":
            new_customer.customer_id = input("Customer ID: ")
        elif choice == "2":
            new_customer.name = input("Name: ")
        elif choice == "3":
            new_customer.address = input("Address: ")
        elif choice == "4":
            new_customer.email = input("Email: ")
        elif choice not in ["5", "6"]:
            print("Invalid choice")

        if choice == "6":
            return

        print()
        print("Current Filters:")
        print("--------------------")
        print(new_customer, end="")
        print("--------------------")
        print()

    found_customers = db.get_filtered_customers(filter_attributes=new_customer, use_patterns=use_patterns)
    print_list_of_objects(found_customers, "customer")


def search_waitlist():
    new_waitlist = Waitlist()
    choice = "1"
    min_place_in_line = -1
    max_place_in_line = -1

    while choice != "5" and choice != "6":
        choice = print_filter_waitlist_menu()
        try:
            if choice == "1":
                new_waitlist.item_id = input("Item ID: ")
            elif choice == "2":
                new_waitlist.customer_id = input("Customer ID: ")
            elif choice == "3":
                min_place_in_line = int(input("Min Place in Line: "))
            elif choice == "4":
                max_place_in_line = int(input("Max Place in Line: "))
            elif choice not in ["5", "6"]:
                print("Unrecognized choice")

        except ValueError:
            print("Please enter a valid integer value")
            print()

        if choice == "6":
            return

        print()
        print("Current Filters:")
        print("--------------------")
        print(new_waitlist, end="")
        if min_place_in_line != -1:
            print(f"Min Place in Line: {min_place_in_line}")
        if max_place_in_line != -1:
            print(f"Max Place in Line: {max_place_in_line}")
        print("--------------------")
        print()

    waitlist_entries = db.get_filtered_waitlist(filter_attributes=new_waitlist, min_place_in_line=min_place_in_line,
                                                max_place_in_line=max_place_in_line)
    print_list_of_objects(waitlist_entries, "waitlisted customer")


def search_rental():
    new_rental = Rental()
    choice = "1"
    min_rental_date = None
    max_rental_date = None
    min_due_date = None
    max_due_date = None

    while choice != "7" and choice != "8":
        choice = print_filter_rental_menu()

        try:
            if choice == "1":
                new_rental.item_id = input("Item ID: ")
            elif choice == "2":
                new_rental.customer_id = input("Customer ID: ")
            elif choice == "3":
                min_rental_date = input("Min Rental Date (YYYY-MM-DD): ")
            elif choice == "4":
                max_rental_date = input("Max Rental Date (YYYY-MM-DD): ")
            elif choice == "5":
                min_due_date = input("Min Due Date (YYYY-MM-DD): ")
            elif choice == "6":
                max_due_date = input("Max Due Date (YYYY-MM-DD): ")
            elif choice not in ["7", "8"]:
                print("Unrecognized choice")

        except ValueError:
            print("Please enter a valid value")
            print()

        if choice == "8":
            return

        print()
        print("Current Filters:")
        print("--------------------")
        print(new_rental, end="")
        if min_rental_date:
            print(f"Min Rental Date: {min_rental_date}")
        if max_rental_date:
            print(f"Max Rental Date: {max_rental_date}")
        if min_due_date:
            print(f"Min Due Date: {min_due_date}")
        if max_due_date:
            print(f"Max Due Date: {max_due_date}")
        print("--------------------")
        print()

    rentals = db.get_filtered_rentals(filter_attributes=new_rental, min_rental_date=min_rental_date,
                                      max_rental_date=max_rental_date, min_due_date=min_due_date,
                                      max_due_date=max_due_date)
    print_list_of_objects(rentals, "rental")


def search_rental_history():
    new_rental_history = RentalHistory()
    choice = "1"
    min_rental_date = None
    max_rental_date = None
    min_due_date = None
    max_due_date = None
    min_return_date = None
    max_return_date = None

    while choice != "9" and choice != "10":
        choice = print_filter_rental_history_menu()

        try:
            if choice == "1":
                new_rental_history.item_id = input("Item ID: ")
            elif choice == "2":
                new_rental_history.customer_id = input("Customer ID: ")
            elif choice == "3":
                min_rental_date = input("Min Rental Date (YYYY-MM-DD): ")
            elif choice == "4":
                max_rental_date = input("Max Rental Date (YYYY-MM-DD): ")
            elif choice == "5":
                min_due_date = input("Min Due Date (YYYY-MM-DD): ")
            elif choice == "6":
                max_due_date = input("Max Due Date (YYYY-MM-DD): ")
            elif choice == "7":
                min_return_date = input("Min Return Date (YYYY-MM-DD): ")
            elif choice == "8":
                max_return_date = input("Max Return Date (YYYY-MM-DD): ")
            elif choice not in ["9", "10"]:
                print("Unrecognized choice")

        except ValueError:
            print("Please enter a valid value")
            print()

        if choice == "10":
            return

        print()
        print("Current Filters:")
        print("--------------------")
        print(new_rental_history, end="")
        if min_rental_date:
            print(f"Min Rental Date: {min_rental_date}")
        if max_rental_date:
            print(f"Max Rental Date: {max_rental_date}")
        if min_due_date:
            print(f"Min Due Date: {min_due_date}")
        if max_due_date:
            print(f"Max Due Date: {max_due_date}")
        if min_return_date:
            print(f"Min Return Date: {min_return_date}")
        if max_return_date:
            print(f"Max Return Date: {max_return_date}")
        print("--------------------")
        print()

    histories = db.get_filtered_rental_histories(filter_attributes=new_rental_history, min_rental_date=min_rental_date,
                                                 max_rental_date=max_rental_date, min_due_date=min_due_date,
                                                 max_due_date=max_due_date, min_return_date=min_return_date,
                                                 max_return_date=max_return_date)
    print_list_of_objects(histories, "return")


def search_tables():
    choice = print_menu("Which table would you like to search?", TABLE_OPTIONS)

    if choice == "1":
        search_items()
    elif choice == "2":
        search_customers()
    elif choice == "3":
        search_rental()
    elif choice == "4":
        search_rental_history()
    elif choice == "5":
        search_waitlist()
    elif choice == "6":
        return
    else:
        print("Invalid choice")


def save_changes():
    db.save_changes()


def close_connection():
    db.close_connection()

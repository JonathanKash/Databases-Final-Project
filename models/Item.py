class Item:
    def __init__(self,
                 item_id: str = None,
                 product_name: str = None,
                 brand: str = None,
                 category: str = None,
                 manufact: str = None,
                 current_price: float = -1,
                 start_year: int = -1,
                 num_owned: int = -1):
        self.item_id = item_id
        self.product_name = product_name
        self.brand = brand
        self.category = category
        self.manufact = manufact
        self.current_price = current_price
        self.start_year = start_year
        self.num_owned = num_owned

    def __str__(self):
        self_str = ""

        if self.item_id:
            self_str += f"Item ID: {self.item_id} \n"
        if self.product_name:
            self_str += f"Product Name: {self.product_name} \n"
        if self.brand:
            self_str += f"Brand: {self.brand} \n"
        if self.category:
            self_str += f"Category: {self.category} \n"
        if self.manufact:
            self_str += f"Manufacturer: {self.manufact} \n"
        if self.current_price != -1:
            self_str += f"Current Price: {self.current_price} \n"
        if self.start_year != -1:
            self_str += f"Start Year: {self.start_year} \n"
        if self.num_owned != -1:
            self_str += f"Total number of copies owned: {self.num_owned} \n"

        return self_str

    def __eq__(self, other):
        return self.item_id == other.item_id

class RentalHistory:
    def __init__(self,
                 item_id: str = None,
                 customer_id: str = None,
                 rental_date: str = None,
                 due_date: str = None,
                 return_date: str = None):
        self.item_id = item_id
        self.customer_id = customer_id
        self.rental_date = rental_date
        self.due_date = due_date
        self.return_date = return_date

    def __str__(self):
        self_str = ""

        if self.item_id:
            self_str += f"Item ID: {self.item_id}\n"
        if self.customer_id:
            self_str += f"Customer ID: {self.customer_id}\n"
        if self.rental_date:
            self_str += f"Rental Date: {self.rental_date}\n"
        if self.due_date:
            self_str += f"Due Date: {self.due_date}\n"
        if self.return_date:
            self_str += f"Return Date: {self.return_date}\n"

        return self_str

    def __eq__(self, other):
        return self.item_id == other.item_id and self.customer_id == other.customer_id and self.rental_date == other.rental_date

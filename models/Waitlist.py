class Waitlist:
    def __init__(self,
                 item_id: str = None,
                 customer_id: str = None,
                 place_in_line: int = -1):
        self.item_id = item_id
        self.customer_id = customer_id
        self.place_in_line = place_in_line

    def __str__(self):
        self_str = ""
        if self.item_id:
            self_str += f"Item ID: {self.item_id} \n"
        if self.customer_id:
            self_str += f"Customer ID: {self.customer_id} \n"
        if self.place_in_line != -1:
            self_str += f"Place in line: {self.place_in_line} \n"

        return self_str

    def __eq__(self, other):
        return self.customer_id == other.customer_id and self.item_id == other.item_id

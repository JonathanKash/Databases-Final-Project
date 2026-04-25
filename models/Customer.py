class Customer:
    def __init__(self,
                 customer_id: str = None,
                 name: str = None,
                 address: str = None,
                 email: str = None):
        self.customer_id = customer_id
        self.name = name
        self.address = address
        self.email = email

    def __str__(self):
        self_str = ""

        if self.customer_id:
            self_str += f"Customer ID: {self.customer_id} \n"
        if self.name:
            self_str += f"Name: {self.name} \n"
        if self.address:
            self_str += f"Address: {self.address} \n"
        if self.email:
            self_str += f"Email: {self.email} \n"

        return self_str

    def __eq__(self, other):
        return self.customer_id == other.customer_id

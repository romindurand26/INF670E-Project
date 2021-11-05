from table import Table_column, Table_row, Row


class Customers_column(Table_column):
    def __init__(self):
        self.primary_key_name = ['id']
        self.foreign_key_name = ['name']
        self.id = []
        self.name = []
        self.age = []

        super().__init__(Customers_row, 'customer')


class Customer(Row):
    def __init__(self, values):
        self.id = None
        self.name = None
        self.age = None

        super().__init__(values)


class Customers_row(Table_row):
    def __init__(self):
        self.primary_key_name = ['id']
        self.foreign_key_name = ['name']
        super().__init__(Customers_column, Customer, 'customer')


storage = 'column'
show = 1

if storage == 'row':
    customers = Customers_row()
else:
    customers = Customers_column()

if show == 1:
    customers.add(["002", "Yasmine", "24"])
    customers.add(["007", "Romin", "77"])
    customers.add(["000", "Oumaima", "5"])
    customers.add(["001", "Louay", "18"])

if show == 2:
    customers.add(["111", "Hubert", "55"])
    customers.add(["222", "Jean", "1"])

if storage == 'row':
    customers.dump_row()
if storage == 'column':
    customers.dump_column()

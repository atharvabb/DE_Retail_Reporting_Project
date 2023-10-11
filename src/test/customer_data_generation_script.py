#This script will generate testing data for customers dimenstion table
#first_name, last_name, address, pincode, phone_number, customer_joining_date

from faker import Faker
import random

fake = Faker()

required_rec = int(input("ENTER NUMBER OF TESTING RECORDS TO BE GENERATED : "))
db_name = str(input("ENTER MYSQL DATABASE NAME"))
for rec in range(required_rec):
    first_name = fake.first_name()
    last_name = fake.last_name()
    address = fake.address()
    pincode = fake.postcode()
    cell_number = "".join([str(random.randint(0,9)) for i in range(10)])
    phone_number = f"{fake.country_calling_code()}-{cell_number}"
    customer_joining_date = fake.date()
    print(f"INSERT INTO {db_name}.customers VALUES({first_name},{last_name},{address},{pincode},{phone_number},{customer_joining_date})")

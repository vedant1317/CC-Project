import uuid
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_USERS = 50_000
emails = set()
duplicates = []

print(f"Generating {NUM_USERS} users and checking for email duplicates...")
for i in range(NUM_USERS):
    email = fake.unique.email()
    if email in emails:
        duplicates.append(email)
    emails.add(email)

if duplicates:
    print(f"Found {len(duplicates)} duplicates!")
    print(f"First few: {duplicates[:5]}")
else:
    print("All emails are unique in the generated set.")

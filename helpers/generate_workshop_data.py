import pandas as pd
from faker import Faker
import random
import uuid
from pathlib import Path

# --- 1. CONFIGURATION ---
NUM_PEOPLE = 1000  # Number of unique individuals to generate
OUTPUT_PATH = Path("synthetic_data")  # Directory to save output files
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
# Initialize the Faker generator

random.seed(42)
Faker.seed(42)

fake = Faker()


def perturb_name(full_name):
    """Applies a random common variation to a person's name."""
    first, last = full_name.split(" ", 1)

    options = [
        # Option 1: Middle initial (if possible)
        f"{first} {random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}. {last}",
        # Option 2: First initial only
        f"{first[0]}. {last}",
        # Option 3: Last name first
        f"{last}, {first}",
        # Option 4: Simple typo
        f"{first[:-2]}{random.choice('aeiou')}{first[-1]} {last}",
        # Option 5: Nickname (simple version)
        (
            f"{random.choice(['Bill', 'Will', 'Billy'])} {last}"
            if first == "William"
            else full_name
        ),
        # Option 6: No change
        full_name,
    ]
    return random.choice(options)


def perturb_address(full_address):
    """Applies a random common variation to an address string."""
    address_parts = full_address.split("\n")
    street_address = address_parts[0]
    city_state_zip = address_parts[1]

    replacements = {"Street": "St.", "Avenue": "Ave.", "Road": "Rd.", "Drive": "Dr."}

    # Option 1: Abbreviate street type
    street_type = random.choice(list(replacements.keys()))
    street_address = street_address.replace(street_type, replacements[street_type])

    options = [
        # Option 1: Abbreviate street type
        street_address,
        # Option 2: Drop the ZIP code
        f"{street_address}\n{city_state_zip.rsplit(' ', 1)[0]}",
        # Option 3: Make a typo in the street name
        f"{street_address[:-5]}{random.choice('aeiou')}{street_address[-4:]}\n{city_state_zip}",
        # Option 4: No change
        full_address,
    ]
    return random.choice(options).replace("\n", ", ")


# --- 3. DATA GENERATION ---

print(f"Generating synthetic data for {NUM_PEOPLE} individuals...")

# Lists to hold the records for each dataset
credit_reports = []
income_records = []
property_records = []
lien_records = []

for _ in range(NUM_PEOPLE):
    # Create a "ground truth" identity for one person
    gov_id = fake.ssn()
    true_name = fake.name()
    true_dob = fake.date_of_birth(minimum_age=25, maximum_age=70)
    true_address = fake.address()

    # --- Generate Credit Report (The "Anchor") ---
    # This record has the cleanest data.
    tradelines = []
    num_accounts = random.randint(2, 8)
    total_monthly_payments = 0
    for _ in range(num_accounts):
        account_type = random.choice(
            ["Credit Card", "Auto Loan", "Mortgage", "Student Loan", "HELOC"]
        )
        balance = 0
        limit = None
        payment = 0
        if account_type == "Credit Card":
            limit = random.choice([2500, 5000, 10000, 15000, 20000])
            balance = round(random.uniform(0.1, 0.9) * limit, 2)
            payment = round(balance * random.uniform(0.02, 0.05), 2)
        elif account_type == "Auto Loan":
            balance = round(random.uniform(5000, 45000), 2)
            payment = round(balance / random.randint(36, 72), 2)
        elif account_type == "Mortgage":
            balance = round(random.uniform(150000, 750000), 2)
            payment = round(balance / random.randint(180, 360), 2)
        else:  # Student Loan or HELOC
            balance = round(random.uniform(10000, 100000), 2)
            payment = round(balance / 120, 2)

        total_monthly_payments += payment
        tradelines.append(
            {
                "account_type": account_type,
                "balance": balance,
                "monthly_payment": payment,
                "credit_limit": limit,
                "utilization_ratio": round(balance / limit, 2) if limit else None,
            }
        )

    credit_reports.append(
        {
            "government_id": gov_id,
            "full_legal_name": true_name,
            "date_of_birth": true_dob,
            "primary_address": true_address.replace("\n", ", "),
            "credit_score": random.randint(450, 850),
            "account_tradelines": tradelines,
        }
    )

    # --- Generate Income Record (Linked by gov_id) ---
    income_records.append(
        {
            "government_id": gov_id,
            "employee_name": perturb_name(true_name),  # Perturbed name
            "employer_name": fake.company(),
            "verified_annual_salary": round(random.uniform(45000, 250000), 2),
        }
    )

    # --- Generate Property Record (Fuzzy Link) ---
    # Not everyone owns property
    if random.random() < 0.7:
        prop_id = str(uuid.uuid4())
        prop_address = true_address  # Start with the true address

        property_records.append(
            {
                "property_id": prop_id,
                "owner_name_on_deed": perturb_name(true_name),  # Perturbed name
                "property_address": perturb_address(prop_address),  # Perturbed address
                "assessed_value": round(random.uniform(200000, 1500000), 2),
            }
        )

        # --- Generate Lien Record (Fuzzy Link on top of Property) ---
        # A small percentage of properties have liens
        if random.random() < 0.15:
            lien_records.append(
                {
                    "lien_id": str(uuid.uuid4()),
                    "property_id": prop_id,
                    "debtor_name": perturb_name(true_name),  # Perturbed again!
                    "debtor_address": perturb_address(prop_address),  # Perturbed again!
                    "lien_holder": random.choice(
                        ["IRS", "State Tax Board", "County Clerk"]
                    ),
                    "lien_amount": round(random.uniform(5000, 75000), 2),
                    "lien_status": "Active",
                }
            )


# --- 4. CREATE AND SAVE DATAFRAMES ---

print("Generation complete. Converting to DataFrames and saving json-lines files...")

# Convert lists of dictionaries to pandas DataFrames
df_credit = pd.DataFrame(credit_reports)
df_income = pd.DataFrame(income_records)
df_property = pd.DataFrame(property_records)
df_liens = pd.DataFrame(lien_records)

# Save to JSON files
df_credit.to_json(
    OUTPUT_PATH / "synthetic_credit_reports.json",
    orient="records",
    lines=True,
)


df_income.to_json(
    OUTPUT_PATH / "synthetic_income_verification.json",
    orient="records",
    lines=True,
)
df_property.to_json(
    OUTPUT_PATH / "synthetic_property_records.json",
    orient="records",
    lines=True,
)
df_liens.to_json(
    OUTPUT_PATH / "synthetic_lien_records.json", orient="records", lines=True
)

print("\nSuccessfully created 4 JSON files:")
print(f" - synthetic_credit_reports.json ({len(df_credit)} rows)")
print(f" - synthetic_income_verification.json ({len(df_income)} rows)")
print(f" - synthetic_property_records.json ({len(df_property)} rows)")
print(f" - synthetic_lien_records.json ({len(df_liens)} rows)")

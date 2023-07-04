import random
import string
from config import first_names, last_names, org_structure, org_names


def mask_only_allowed_values(account_number, length, extra_params):
    allowed_values = extra_params.get("allowed_values")
    return random.choice(allowed_values)

def mask_add_values(account_number, length, extra_params):
    print(account_number)
    # Check if the account_number is an empty string
    if account_number == "nan":
        return account_number.replace("nan", "")
    else:
        # Convert the account number to a float, then to an integer, add 1000, then convert a string
        return str(int(float(account_number)) + 1000)


def mask_first_name(account_number, length, extra_params):
    return random.choice(first_names)


def mask_last_name(account_number, length, extra_params):
    return random.choice(last_names)


def mask_any_name(account_number, length, extra_params):
    separator = extra_params.get("separator", " ")
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    return f"{first_name}{separator}{last_name}"


def mask_org_name(account_number, length, extra_params):
    separator = extra_params.get("separator", " ")
    first_name = random.choice(org_names)
    last_name = random.choice(org_structure)
    return f"{first_name}{separator}{last_name}"


def mask_acct(account_number, length, extra_params):
    if len(account_number) == length:
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))
    else:
        return account_number


def mask_integer(account_number, length, extra_params):
    return "".join(random.choices(string.digits, k=length))


def mask_default(account_number, length, extra_params):
        print("here")
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


# Defining columns to be treated special 
mask_functions = {
    "ID2": mask_add_values,
}

columns_to_mask = {
    "ID2": ("VARCHAR", None, None),
}

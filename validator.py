import datetime

valid_cities = {"Mumbai", "Bangalore"}
date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]

def validate_row(row, product_map):
    reasons = []
    for field, value in row.items():
        if value.strip() == "":
            reasons.append(f"{field} is empty")

    if row["product_id"] not in product_map:
        reasons.append("Invalid product_id")

    try:
        price = product_map.get(row["product_id"], 0)
        qty = int(row["quantity"])
        sales = int(row["sales"])
        if sales != price * qty:
            reasons.append("Incorrect sales amount")
    except Exception:
        reasons.append("Invalid quantity or sales")

    if row["city"] not in valid_cities:
        reasons.append("Invalid city")

    order_date_str = row.get("order_date", "")
    parsed_date = None
    for format in date_formats:
        try:
            parsed_date = datetime.datetime.strptime(order_date_str, format).date()
            break
        except ValueError:
            continue

    if not parsed_date:
        reasons.append("Invalid date format")
    elif parsed_date > datetime.date.today():
        reasons.append("Order date is in future")

    return reasons

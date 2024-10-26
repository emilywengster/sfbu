import json

# Sample products database for testing purposes (replace with your actual data)
products = {
    "TechPro Ultrabook": {
        "name": "TechPro Ultrabook",
        "category": "Computers and Laptops",
        "brand": "TechPro",
        "description": "A sleek and lightweight ultrabook for everyday use.",
        "price": 799.99
    },
    "SmartX ProPhone": {
        "name": "SmartX ProPhone",
        "category": "Smartphones and Accessories",
        "brand": "SmartX",
        "description": "A powerful smartphone with advanced camera features.",
        "price": 899.99
    },
    # Add more products as needed...
}

def get_products_from_query(query):
    """
    Parses the user's query to identify products or categories mentioned.
    Returns a list of dictionaries with product and category names found in the query.
    """
    mentioned_products = []
    for product_name, product_info in products.items():
        if product_name.lower() in query.lower() or product_info["category"].lower() in query.lower():
            mentioned_products.append({"category": product_info["category"], "product": product_name})
    return mentioned_products

def read_string_to_list(input_data):
    """
    Converts a JSON-like string into a Python list of dictionaries.
    If input_data is already a list, it returns it directly.
    """
    if isinstance(input_data, list):
        return input_data
    try:
        # Parse string input
        return json.loads(input_data)
    except json.JSONDecodeError:
        print("Error: Invalid JSON string")
        return None
def get_mentioned_product_info(mentioned_products):
    """
    Retrieves detailed product information based on the list of mentioned products.
    Returns a dictionary containing detailed info for each mentioned product.
    """
    product_info = []
    for item in mentioned_products:
        product_name = item["product"]
        if product_name in products:
            product_info.append(products[product_name])
    return product_info

def answer_user_msg(user_msg, product_info):
    """
    Generates a response message for the user based on their message and the gathered product information.
    Returns a string with a tailored response.
    """
    if not product_info:
        return "I'm sorry, I couldn't find any products that match your query."

    response = "Here are the details for the products you mentioned:\n"
    for product in product_info:
        response += (
            f"\nProduct: {product['name']}\n"
            f"Category: {product['category']}\n"
            f"Brand: {product['brand']}\n"
            f"Description: {product['description']}\n"
            f"Price: ${product['price']}\n"
        )
    return response

def get_products_and_category():
    """
    Returns a dictionary with categories as keys and lists of products as values.
    """
    products_by_category = {}
    for product_name, product_info in products.items():
        category = product_info["category"]
        if category not in products_by_category:
            products_by_category[category] = []
        products_by_category[category].append({
            "name": product_info["name"],
            "brand": product_info["brand"],
            "description": product_info["description"],
            "price": product_info["price"]
        })
    return products_by_category

def eval_response_with_ideal(response, ideal, debug=False):
    json_like_str = response  # Assuming `response` is your JSON-like input

    if debug:
        print("Original JSON-like string:", json_like_str)

    # Attempt to fix JSON formatting issues
    try:
        # Replace single quotes with double quotes (quick fix)
        json_like_str = json_like_str.replace("'", "\"")

        # Convert to JSON object
        l_of_d = json.loads(json_like_str)
        return l_of_d
    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)
        print("Error at character:", e.pos)
        print("Partial JSON string:", json_like_str[:e.pos + 20])  # Display around the error location
        raise e
    


from flask import Flask, render_template, request
import openai

# Initialize Flask app
app = Flask(__name__)

# Set your OpenAI API key here
api_key = ''

# Product descriptions dictionary
products = {
    "TechPro Ultrabook": {
        "name": "TechPro Ultrabook",
        "category": "Computers and Laptops",
        "brand": "TechPro",
        "model_number": "TP-UB100",
        "warranty": "1 year",
        "rating": 4.5,
        "features": ["13.3-inch display", "8GB RAM", "256GB SSD", "Intel Core i5 processor"],
        "description": "A sleek and lightweight ultrabook for everyday use.",
        "price": 799.99
    },
    "BlueWave Gaming Laptop": {
        "name": "BlueWave Gaming Laptop",
        "category": "Computers and Laptops",
        "brand": "BlueWave",
        "model_number": "BW-GL200",
        "warranty": "2 years",
        "rating": 4.7,
        "features": ["15.6-inch display", "16GB RAM", "512GB SSD", "NVIDIA GeForce RTX 3060"],
        "description": "A high-performance gaming laptop for an immersive experience.",
        "price": 1199.99
    },
    "PowerLite Convertible": {
        "name": "PowerLite Convertible",
        "category": "Computers and Laptops",
        "brand": "PowerLite",
        "model_number": "PL-CV300",
        "warranty": "1 year",
        "rating": 4.3,
        "features": ["14-inch touchscreen", "8GB RAM", 
                  "256GB SSD", "360-degree hinge"],
        "description": "A versatile convertible laptop with a responsive touchscreen.",
        "price": 699.99
    },
    "TechPro Desktop": {
        "name": "TechPro Desktop",
        "category": "Computers and Laptops",
        "brand": "TechPro",
        "model_number": "TP-DT500",
        "warranty": "1 year",
        "rating": 4.4,
        "features": ["Intel Core i7 processor", "16GB RAM", 
                  "1TB HDD", "NVIDIA GeForce GTX 1660"],
        "description": "A powerful desktop computer for work and play.",
        "price": 999.99
    },
    "BlueWave Chromebook": {
        "name": "BlueWave Chromebook",
        "category": "Computers and Laptops",
        "brand": "BlueWave",
        "model_number": "BW-CB100",
        "warranty": "1 year",
        "rating": 4.1,
        "features": ["11.6-inch display", "4GB RAM", 
                  "32GB eMMC", 
                  "Chrome OS"],
        "description": "A compact and affordable Chromebook for everyday tasks.",
        "price": 249.99
    },
    "SmartX ProPhone": {
        "name": "SmartX ProPhone",
        "category": "Smartphones and Accessories",
        "brand": "SmartX",
        "model_number": "SX-PP10",
        "warranty": "1 year",
        "rating": 4.6,
        "features": ["6.1-inch display", "128GB storage", 
                  "12MP dual camera", "5G"],
        "description": "A powerful smartphone with advanced camera features.",
        "price": 899.99
    },
    "MobiTech PowerCase": {
        "name": "MobiTech PowerCase",
        "category": "Smartphones and Accessories",
        "brand": "MobiTech",
        "model_number": "MT-PC20",
        "warranty": "1 year",
        "rating": 4.3,
        "features": 
             ["5000mAh battery", "Wireless charging", 
             "Compatible with SmartX ProPhone"],
        "description": 
             "A protective case with built-in battery for extended usage.",
        "price": 59.99
    },
    "SmartX MiniPhone": {
        "name": "SmartX MiniPhone",
        "category": "Smartphones and Accessories",
        "brand": "SmartX",
        "model_number": "SX-MP5",
        "warranty": "1 year",
        "rating": 4.2,
        "features": ["4.7-inch display", "64GB storage", 
                  "8MP camera", "4G"],
        "description": "A compact and affordable smartphone for basic tasks.",
        "price": 399.99
    },
    "MobiTech Wireless Charger": {
        "name": "MobiTech Wireless Charger",
        "category": "Smartphones and Accessories",
        "brand": "MobiTech",
        "model_number": "MT-WC10",
        "warranty": "1 year",
        "rating": 4.5,
        "features": ["10W fast charging", "Qi-compatible", 
                  "LED indicator", "Compact design"],
        "description": "A convenient wireless charger for a clutter-free workspace.",
        "price": 29.99
    },
    "SmartX EarBuds": {
        "name": "SmartX EarBuds",
        "category": "Smartphones and Accessories",
        "brand": "SmartX",
        "model_number": "SX-EB20",
        "warranty": "1 year",
        "rating": 4.4,
        "features": ["True wireless", "Bluetooth 5.0", 
                  "Touch controls", "24-hour battery life"],
        "description": "Experience true wireless freedom with these comfortable earbuds.",
        "price": 99.99
    },

    "CineView 4K TV": {
        "name": "CineView 4K TV",
        "category": "Televisions and Home Theater Systems",
        "brand": "CineView",
        "model_number": "CV-4K55",
        "warranty": "2 years",
        "rating": 4.8,
        "features": ["55-inch display", "4K resolution", 
                  "HDR", "Smart TV"],
        "description": "A stunning 4K TV with vibrant colors and smart features.",
        "price": 599.99
    },
    "SoundMax Home Theater": {
        "name": "SoundMax Home Theater",
        "category": "Televisions and Home Theater Systems",
        "brand": "SoundMax",
        "model_number": "SM-HT100",
        "warranty": "1 year",
        "rating": 4.4,
        "features": ["5.1 channel", "1000W output", 
                  "Wireless subwoofer", "Bluetooth"],
        "description": "A powerful home theater system for an immersive audio experience.",
        "price": 399.99
    },
    "CineView 8K TV": {
        "name": "CineView 8K TV",
        "category": "Televisions and Home Theater Systems",
        "brand": "CineView",
        "model_number": "CV-8K65",
        "warranty": "2 years",
        "rating": 4.9,
        "features": ["65-inch display", "8K resolution", 
                  "HDR", "Smart TV"],
        "description": "Experience the future of television with this stunning 8K TV.",
        "price": 2999.99
    },
    "SoundMax Soundbar": {
        "name": "SoundMax Soundbar",
        "category": "Televisions and Home Theater Systems",
        "brand": "SoundMax",
        "model_number": "SM-SB50",
        "warranty": "1 year",
        "rating": 4.3,
        "features": ["2.1 channel", "300W output", 
                  "Wireless subwoofer", "Bluetooth"],
        "description": "Upgrade your TV's audio with this sleek and powerful soundbar.",
        "price": 199.99
    },
    "CineView OLED TV": {
        "name": "CineView OLED TV",
        "category": "Televisions and Home Theater Systems",
        "brand": "CineView",
        "model_number": "CV-OLED55",
        "warranty": "2 years",
        "rating": 4.7,
        "features": ["55-inch display", "4K resolution", 
                  "HDR", "Smart TV"],
        "description": "Experience true blacks and vibrant colors with this OLED TV.",
        "price": 1499.99
    },

    "GameSphere X": {
        "name": "GameSphere X",
        "category": "Gaming Consoles and Accessories",
        "brand": "GameSphere",
        "model_number": "GS-X",
        "warranty": "1 year",
        "rating": 4.9,
        "features": ["4K gaming", "1TB storage", 
                  "Backward compatibility", 
                  "Online multiplayer"],
        "description": 
                  "A next-generation gaming console for the ultimate gaming experience.",
        "price": 499.99
    },
    "ProGamer Controller": {
        "name": "ProGamer Controller",
        "category": "Gaming Consoles and Accessories",
        "brand": "ProGamer",
        "model_number": "PG-C100",
        "warranty": "1 year",
        "rating": 4.2,
        "features": ["Ergonomic design", 
                  "Customizable buttons", 
                  "Wireless", "Rechargeable battery"],
        "description": "A high-quality gaming controller for precision and comfort.",
        "price": 59.99
    },
    "GameSphere Y": {
        "name": "GameSphere Y",
        "category": "Gaming Consoles and Accessories",
        "brand": "GameSphere",
        "model_number": "GS-Y",
        "warranty": "1 year",
        "rating": 4.8,
        "features": ["4K gaming", "500GB storage", 
                  "Backward compatibility", 
                  "Online multiplayer"],
        "description": "A compact gaming console with powerful performance.",
        "price": 399.99
    },
    "ProGamer Racing Wheel": {
        "name": "ProGamer Racing Wheel",
        "category": "Gaming Consoles and Accessories",
        "brand": "ProGamer",
        "model_number": "PG-RW200",
        "warranty": "1 year",
        "rating": 4.5,
        "features": ["Force feedback", "Adjustable pedals", 
                  "Paddle shifters", 
                  "Compatible with GameSphere X"],
        "description": "Enhance your racing games with this realistic racing wheel.",
        "price": 249.99
    },
    "GameSphere VR Headset": {
        "name": "GameSphere VR Headset",
        "category": "Gaming Consoles and Accessories",
        "brand": "GameSphere",
        "model_number": "GS-VR",
        "warranty": "1 year",
        "rating": 4.6,
        "features": ["Immersive VR experience", 
            "Built-in headphones", 
            "Adjustable headband", 
            "Compatible with GameSphere X"],
        "description": 
             "Step into the world of virtual reality with this comfortable VR headset.",
        "price": 299.99
    },

    "AudioPhonic Noise-Canceling Headphones": {
        "name": "AudioPhonic Noise-Canceling Headphones",
        "category": "Audio Equipment",
        "brand": "AudioPhonic",
        "model_number": "AP-NC100",
        "warranty": "1 year",
        "rating": 4.6,
        "features": ["Active noise-canceling", "Bluetooth", 
                  "20-hour battery life", "Comfortable fit"],
        "description": "Experience immersive sound with these noise-canceling headphones.",
        "price": 199.99
    },
    "WaveSound Bluetooth Speaker": {
        "name": "WaveSound Bluetooth Speaker",
        "category": "Audio Equipment",
        "brand": "WaveSound",
        "model_number": "WS-BS50",
        "warranty": "1 year",
        "rating": 4.5,
        "features": ["Portable", "10-hour battery life", 
                  "Water-resistant", "Built-in microphone"],
        "description": "A compact and versatile Bluetooth speaker for music on the go.",
        "price": 49.99
    },
    "AudioPhonic True Wireless Earbuds": {
        "name": "AudioPhonic True Wireless Earbuds",
        "category": "Audio Equipment",
        "brand": "AudioPhonic",
        "model_number": "AP-TW20",
        "warranty": "1 year",
        "rating": 4.4,
        "features": ["True wireless", "Bluetooth 5.0", 
                  "Touch controls", "18-hour battery life"],
        "description": "Enjoy music without wires with these comfortable true wireless earbuds.",
        "price": 79.99
    },
    "WaveSound Soundbar": {
        "name": "WaveSound Soundbar",
        "category": "Audio Equipment",
        "brand": "WaveSound",
        "model_number": "WS-SB40",
        "warranty": "1 year",
        "rating": 4.3,
        "features": ["2.0 channel", "80W output", 
                  "Bluetooth", "Wall-mountable"],
        "description": "Upgrade your TV's audio with this slim and powerful soundbar.",
        "price": 99.99
    },
    "AudioPhonic Turntable": {
        "name": "AudioPhonic Turntable",
        "category": "Audio Equipment",
        "brand": "AudioPhonic",
        "model_number": "AP-TT10",
        "warranty": "1 year",
        "rating": 4.2,
        "features": ["3-speed", "Built-in speakers", 
                  "Bluetooth", "USB recording"],
        "description": "Rediscover your vinyl collection with this modern turntable.",
        "price": 149.99
    },

    "FotoSnap DSLR Camera": {
        "name": "FotoSnap DSLR Camera",
        "category": "Cameras and Camcorders",
        "brand": "FotoSnap",
        "model_number": "FS-DSLR200",
        "warranty": "1 year",
        "rating": 4.7,
        "features": ["24.2MP sensor", "1080p video", 
              "3-inch LCD", 
              "Interchangeable lenses"],
        "description": 
              "Capture stunning photos and videos with this versatile DSLR camera.",
        "price": 599.99
    },
    "ActionCam 4K": {
        "name": "ActionCam 4K",
        "category": "Cameras and Camcorders",
        "brand": "ActionCam",
        "model_number": "AC-4K",
        "warranty": "1 year",
        "rating": 4.4,
        "features": ["4K video", "Waterproof", 
                  "Image stabilization", "Wi-Fi"],
        "description": "Record your adventures with this rugged and compact 4K action camera.",
        "price": 299.99
    },
    "FotoSnap Mirrorless Camera": {
        "name": "FotoSnap Mirrorless Camera",
        "category": "Cameras and Camcorders",
        "brand": "FotoSnap",
        "model_number": "FS-ML100",
        "warranty": "1 year",
        "rating": 4.6,
        "features": ["20.1MP sensor", "4K video", 
             "3-inch touchscreen", 
             "Interchangeable lenses"],
        "description": "A compact and lightweight mirrorless camera with advanced features.",
        "price": 799.99
    },
    "ZoomMaster Camcorder": {
        "name": "ZoomMaster Camcorder",
        "category": "Cameras and Camcorders",
        "brand": "ZoomMaster",
        "model_number": "ZM-CM50",
        "warranty": "1 year",
        "rating": 4.3,
        "features": ["1080p video", "30x optical zoom", 
                  "3-inch LCD", "Image stabilization"],
        "description": "Capture life's moments with this easy-to-use camcorder.",
        "price": 249.99
    },
    "FotoSnap Instant Camera": {
        "name": "FotoSnap Instant Camera",
        "category": "Cameras and Camcorders",
        "brand": "FotoSnap",
        "model_number": "FS-IC10",
        "warranty": "1 year",
        "rating": 4.1,
        "features": ["Instant prints", "Built-in flash", 
                  "Selfie mirror", "Battery-powered"],
        "description": "Create instant memories with this fun and portable instant camera.",
        "price": 69.99
    },
}
"""
# Function to generate a 100-word comment based on product descriptions
def generate_comment_for_products(selected_products):
    # Create a prompt from the selected product details
    prompt = "Please generate a 100-word comment based on the following product details:\n\n"
    
    client = openai.Client(api_key=api_key)

    for product in selected_products:
        product_info = products[product]
        # Include the important fields in the prompt
        prompt += (
            f"Product: {product_info['name']}\n"
            f"Category: {product_info['category']}\n"
            f"Brand: {product_info['brand']}\n"
            f"Model Number: {product_info['model_number']}\n"
            f"Warranty: {product_info['warranty']}\n"
            f"Rating: {product_info['rating']}\n"
            f"Features: {', '.join(product_info['features'])}\n"
            f"Description: {product_info['description']}\n"
            f"Price: ${product_info['price']}\n\n"
        )

    # Send the prompt to OpenAI and generate the comment
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",  # You can use "gpt-4" if you have access
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,         # Limit the length of the response
            temperature=0.7         # Controls creativity/randomness (higher is more random)
        )
        
        # Extract the response
        gpt_response = response.choices[0].message.content.strip()
        return gpt_response

    except Exception as e:
        return f"An error occurred: {e}"

# Function to interact with the user
def chat_with_gpt():
    print("Welcome to the Product Comment Generator! (Type 'exit' to quit)\n")
    print(f"Available products: {', '.join(products.keys())}\n")

    while True:
        # Get user input (product names)
        user_input = input("Enter product names separated by commas (or type 'exit' to quit): ")
        
        # Check if the user wants to exit
        if user_input.lower() == 'exit':
            print("Goodbye!")
            break
        
        # Get a list of selected products from the user input
        selected_products = [p.strip() for p in user_input.split(",") if p.strip() in products]
        
        if not selected_products:
            print("No valid products selected. Please try again.\n")
            continue
        
        # Generate the comment for the selected products
        comment = generate_comment_for_products(selected_products)
        print(f"\nChatGPT Comment:\n{comment}\n")

# Start the chat interface
if __name__ == "__main__":
    chat_with_gpt()
"""
# Map language codes to human-readable languages for prompt generation
language_map = {
    "en": "English",
    "es": "Spanish",
    "zh": "Chinese",
    "tl": "Tagalog",
    "vi": "Vietnamese",
    "ar": "Arabic",
    "fr": "French"
}


@app.route('/', methods=['GET', 'POST'])
def index():

    question = ""
    client = openai.Client(api_key=api_key)
    answer = None
    if request.method == 'POST':
        # Get question and language from the form
        question = request.form['question']
        language = request.form['language']
        language_name = language_map.get(language, "English")
        
        # Generate the response from ChatGPT based on the question
        try:
            prompt = f"Please respond in {language_name}. The question is: {question}"

            response = client.chat.completions.create(
                model="gpt-3.5-turbo",  # You can use "gpt-4" if you have access
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.7
            )
            # Extract the response
            answer = response.choices[0].message.content.strip()

        except Exception as e:
            answer = f"An error occurred: {e}"

    return render_template('index.html', answer=answer)

if __name__ == '__main__':
    app.run(debug=True)
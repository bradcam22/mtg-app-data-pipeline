import json
from mtgsdk import Card

# Fetch all cards
cards = Card.all()

# Create a list to hold card dictionaries
card_list = []

# Loop through all cards and convert each to a dictionary
for card in cards:
    card_list.append(card.__dict__)

# Save the list of cards directly to a JSON file without wrapping it
with open("/workspaces/mtg_app/data/src_cards.json", "w") as json_file:
    json.dump(card_list, json_file, indent=4)

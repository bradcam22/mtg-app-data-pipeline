# Saving rules from the official Magic: The Gathering website
# Manually splitting into two files: glossary.txt and rules.txt (automate later)

import requests

url = "https://media.wizards.com/2024/downloads/MagicCompRules20240917.txt"
target = "/workspaces/mtg_app/data/MagicCompRulesAndGlossary.txt"

response = requests.get(url)

with open(target, "wb") as target_file:
    target_file.write(response.content)

print(f"Rules saved to {target}")

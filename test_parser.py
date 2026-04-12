from parser.title_parser import parse_rain_title

titles = [
    "Will it rain in New York City on April 14?",
    "Will there be significant rainfall in NYC soon?",
    "Will it rain in NYC and Boston tomorrow?",
    "Will it rain at JFK Airport tomorrow?"
]

for title in titles:
    result = parse_rain_title(title)
    print("TITLE:", title)
    print("RESULT:", result)
    print("-" * 40)
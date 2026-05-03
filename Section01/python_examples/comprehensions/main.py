nums = [1, 2, 3, 4, 5, 6, 7, 8, 9]
squares = [i * i for i in nums ]

print(squares) # [1, 4, 9, 16, 25, 36, 49, 64, 81]

raw_times = ["112ms", "998ms", "101ms", "115ms", "1904ms", "362ms", "231ms"]

response_times = []
for t in raw_times:
    cleaned = int(t.rstrip("ms"))
    response_times.append(cleaned)

print(response_times) # [112, 998, 101, 115, 1904, 362, 231]

response_times = [int(t.rstrip("ms")) for t in raw_times]
print(response_times) # [112, 998, 101, 115, 1904, 362, 231]

slow_response_times = [t for t in response_times if t > 500]
print(slow_response_times) # [998, 1904]

records = [
    {"id": 1001, "city": "London", "temp": 21.7},
    {"id": 1002, "city": "New York", "temp": 19.3},
    {"id": 1033, "city": "Berlin", "temp": 20.1},
]

temp_for_city = {}
for row in records:
    temp_for_city[row["city"]] = row["temp"]

print(temp_for_city) # {'London': 21.7, 'New York': 19.3, 'Berlin': 20.1}

city_temp = {row["city"]: row["temp"] for row in records}
print(city_temp) # {'London': 21.7, 'New York': 19.3, 'Berlin': 20.1}
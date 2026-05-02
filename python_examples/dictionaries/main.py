page_visits_count = {
    "/info": 1458,
    "/search": 47,
    "/order": 8,
}

# print(page_visits_count) # {'/info': 1458, '/search': 47, '/order': 8}
# print(page_visits_count["/info"]) # 1458

# print(page_visits_count.get("/info")) # 1458
# print(page_visits_count.get("/unknown")) # None
# print(page_visits_count.get("/payments", 0)) # 0

# for item in page_visits_count.items():
#     print(item[0], " -> ", item[1])
# # /info  ->  1458
# # /search  ->  47
# # /order  ->  8

# for page, visits_count in page_visits_count.items():
#     print(page, " -> ", visits_count)
#     # /info  ->  1458
#     # /search  ->  47
#     # /order  ->  8

page_visits_count["/contact"] = 20
# print(page_visits_count) # {'/info': 1458, '/search': 47, '/order': 8, '/contact': 20}

visits = [
    ("alice", "/info"),
    ("bob", "/info"),
    ("alice", "/order"),
    ("carol", "/info"),
    ("bob", "/search"),
]

page_counts = {}
for _, page in visits:
    page_counts[page] = page_counts.get(page, 0) + 1

print(page_counts) # {'/info': 3, '/order': 1, '/search': 1}


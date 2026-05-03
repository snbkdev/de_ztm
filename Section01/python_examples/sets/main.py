visited_urls = {
    "/info", "/order", "/search"
}

# print(visited_urls) # {'/order', '/search', '/info'}

visited_urls.add("/search")
visited_urls.add("/payments")
# print(visited_urls) # {'/payments', '/info', '/search', '/order'}

# print("/search" in visited_urls) # True
# print("/pages" in visited_urls) # False

visited_urls.remove("/order")
print(visited_urls)

visited_urls_list = ["/info", "/order", "/search"]
set(visited_urls_list)

empty_set = set() # {} means an empty dictionary

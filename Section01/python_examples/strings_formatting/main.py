user = "Alice"
msg = "Hello, " + user + "!"
print(msg) # Hello, Alice!

page_name = "/users"
visits_count = 15
msg_ = "Number of visits for " + page_name + " is: " + str(visits_count)
print(msg_) # Number of visits for /users is: 15

print("Number of visits for {} is -  {}".format(page_name, visits_count)) # Number of visits for /users is -  15
print(f"Number of visits for {page_name} is -  {visits_count}") # Number of visits for /users is -  15

message= f"""
Page name :{page_name}
Visit counts: {visits_count}
"""
print(message)
# Page name :/users
# Visit counts: 15

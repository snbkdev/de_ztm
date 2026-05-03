
def log(msg, level="INFO"):
    print(f'[{level}] {msg}')

# log("text")
# log("text", level="DEBUG")
# log("text", "WARN")

def greet_all(greeting, *names):
    for n in names:
        print(f"{greeting}, {n}!")

# greet_all("Hi", "Ada", "Leon", "Grace")

def min_and_max(*nums):
    min_value = min(nums)
    max_value = max(nums)

    return (min_value, max_value)

min_value, max_value = min_and_max(1, 2, 3, 4, 5, 6, 7, 8, 9)
print(min_value, max_value) # 1 9


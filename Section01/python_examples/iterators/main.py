def greeter():
    yield "hello"
    yield "world"


g = greeter()
# print(next(g)) # hello
# print(next(g)) # world
# print(next(g)) # StopIteration

def greeter_with_prints():
    # print("msg: start")
    yield "hello"
    # print("msg: after first yield")
    yield "world"

c = greeter_with_prints()
# print(next(c))
# print(next(c))

b = greeter()
for m in greeter():
    print(m)


def read_lines(path):
    with open(path, "r") as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line != "":
                yield line.rstrip("\n")

csv_data = """
alice, /, 112
bob, /about, 98
carol, /payment, 250
"""

out_path = "demo.csv"

out_csv = open(out_path, "w", newline="")
out_csv.write(csv_data)
out_csv.close()

lines = read_lines("demo.csv")
# print(next(lines)) # alice, /, 112
# print(next(lines)) # bob, /about, 98

nums = [10, -2, 3, 8, -1, -11]
print(max(i * i for i in nums))
def file_length(file_path):
    try:
        f = open(file_path)

        data = f.read()
        f.close()
        return len(data)
    except FileNotFoundError as e:
        return 0
    finally:
        if f is not None:
            f.close()

csv_data = """"user,page, response_ms
alice,/,112
bob,/about,98
"""

out_path = "demo.csv"
out_csv = open(out_path, "w")
out_csv.write(csv_data)
out_csv.close()

print(file_length(out_path))
print(file_length("demo2.csv"))

file_length(None)

def file_length2(log_path):
    try:
        with open(log_path) as f:
            return len(f.read())
    except FileNotFoundError as e:
        return 0
    

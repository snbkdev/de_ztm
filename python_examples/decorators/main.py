def transform_all(items, fn):
    return [fn(x) for x in items]

def normalize(s):
    return s.strip().lower()

raw = ["Apple", "Banana", "kiwi", "orange"]
# print(transform_all(raw, normalize)) # ['apple', 'banana', 'kiwi', 'orange']
# print(transform_all(raw, lambda s: s.strip().lower())) # ['apple', 'banana', 'kiwi', 'orange']

def make_text_cleaner(strip=True, lower=True, remove_commas=False):
    def _clean(s):
        if strip: s = s.strip()
        if lower: s = s.lower()
        if remove_commas: s = s.replace(",", "")
        return s
    return _clean

clean_spaces = make_text_cleaner(strip=True, lower=False, remove_commas=True)
# print(clean_spaces("    omg      "))
# print(clean_spaces("wow, "))

import hashlib
from functools import cache

@cache
def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

import time

BIG_BLOB = ("data-" * 2_000_000).encode()

t = time.time()
sha256_hex(BIG_BLOB)
diff = time.time() - t

print(f"{diff:.6f}s")
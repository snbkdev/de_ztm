import math

class Point2D:
    def __init__(self, x=0, y=0):
        self.x = x
        self.y = y

    def move(self, dx, dy):
        self.x += dx
        self.y += dy

    @staticmethod
    def distance(p1, p2):
        dx = p1.x - p2.x
        dy = p1.y - p2.y
        return math.sqrt(dx * dx + dy * dy)

p = Point2D(1, 1)
p.move(2, 2)

# print(p.x, p.y) # 3 3

p1 = Point2D(1, 1)
p2 = Point2D(3, 3)

Point2D.distance(p1, p2)
# print(Point2D.distance(p1, p2)) # 2.8284271247461903

class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance

    def withdraw(self, amount):
        self.balance -= amount
        return self.balance


class SafeAccount(BankAccount):
    def withdraw(self, amount):
        if (self.balance - amount) < 0:
            print("Final balance must be non-negative")
            return self.balance
        
        self.balance -= amount
        return self.balance
    
acct = BankAccount(100)
# print("After -50: ", acct.withdraw(50))
# print("After -200: ", acct.withdraw(200))
# print("Balance: ", acct.balance)

# print("---------------------------|| -- ||---------------------------")

safe_acct = SafeAccount(100)
# print("After -50: ", safe_acct.withdraw(50))
# print("After -200 (blocked) : ", safe_acct.withdraw(200))
# print("Balance: ", safe_acct.balance)

from dataclasses import dataclass

@dataclass
class Point3D:
    x: float
    y: float

a = Point3D(2, 3)
print(a) # Point3D(x=2, y=3)
print(a == Point3D(2, 3)) # True
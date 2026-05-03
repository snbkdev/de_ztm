l = [1, 2, 3, 4, 5]
l.append(6)
l.append(7)

class BrowserTab:
    def __init__(self, url="about:blank", pinned=False):
        self.url = url
        self.pinned = pinned
        self.history = [url]

b = BrowserTab(url="https://zerotomastery.io/", pinned=False)
# print(f" url={b.url}, history={b.history}, pinned={b.pinned}")
# url=https://zerotomastery.io/, history=['https://zerotomastery.io/'], pinned=False

b.url = "https://google.com"
# print(f" url={b.url}, history={b.history}, pinned={b.pinned}")
#  url=https://google.com, history=['https://zerotomastery.io/'], pinned=False

class BrowserTab2:
    def __init__(self, url="about:blank", pinned=False):
        self.url = url
        self.pinned = pinned
        self.history = [url]

    def navigate(self, url):
        self.url = url
        self.history.append(url)


c = BrowserTab2()
c.navigate("https://reddit.com")
# print(f" url={c.url}, history={c.history}, pinned={c.pinned}")
# url=https://reddit.com, history=['about:blank', 'https://reddit.com'], pinned=False

class Point2D:
    def __init__(self, x=0, y=0):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Point2D(x={self.x}, y={self.y})"
    
    def __str__(self):
        return f"({self.x}, {self.y})"

    def __eq__(self, other):
        if not isinstance(other, Point2D):
            return NotImplemented
        return (self.x, self.y) == (other.x, other.y)
    
    def __hash__(self):
        return hash((self.x, self.y))
    
a = Point2D(5, 2)
print(a)
print(a == Point2D(5, 4)) # False
print(a == Point2D(5, 2)) # True


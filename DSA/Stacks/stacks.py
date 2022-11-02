class Stack:
    def __init__(self):
        self.items = []
    def is_empty(self):
        return self.items == []
    def push(self, item):
        self.items.insert(0, item)
    def pop(self):
        return self.items.pop(0)
    def peek(self):
        return self.items[0]
    def size(self):
        return len(self.items)

def per_checker(symbol_string):

    s = Stack()
    balanced = True
    index = 0
    while index < len(symbol_string) and balanced:
        symbol = symbol_string[index]
        if symbol == "(":
            s.push(symbol)
            print(s.items)
        else:
            if s.is_empty():
                balanced = False
            else:
                s.pop()
                print(s)

            index = index + 1

        if balanced and s.is_empty():
            return True
        else:
            return False

print(per_checker('((()))'))
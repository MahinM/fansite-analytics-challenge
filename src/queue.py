#Reference:
#http://openbookproject.net/thinkcs/python/english3e/queues.html


#Node structure     
class Node:
    def __init__(self, value=None, next=None):
        self.value = value
        self.next  = next

    def __str__(self):
        return str(self.value)

class Queue:
    def __init__(self):
        self.length = 0
        self.head = None

    def is_empty(self):
        return self.length == 0

    def insert(self, value):
        node = Node(value)
        if self.head is None:
            # If list is empty the new node goes first
            self.head = node
        else:
            # Find the last node in the list
            last = self.head
            while last.next:
                last = last.next
            # Append the new node
            last.next = node
        self.length += 1

    def remove(self):
        value = self.head.value
        self.head = self.head.next
        self.length -= 1
        return value
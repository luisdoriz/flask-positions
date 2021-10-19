import time
from threading import Thread

class Compute(Thread):
    def __init__(self, request, class_name):
        Thread.__init__(self)
        self.request = request
        self.class_name = class_name

    def run(self):
        print("start")
        class_instance = self.class_name()
        class_instance.main()
        print("done")

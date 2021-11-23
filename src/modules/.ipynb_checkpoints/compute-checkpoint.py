import time
from threading import Thread

class Compute(Thread):
    def __init__(self, request, class_name, params=None):
        Thread.__init__(self)
        self.request = request
        self.class_name = class_name
        self.params = params

    def run(self):
        print("start")
        if self.params is None:
            class_instance = self.class_name()
        else:
            class_instance = self.class_name(self.params)
        class_instance.main()
        print("done")

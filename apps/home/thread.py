import time
from threading import Thread
from flask import request, Flask
app = Flask(__name__)


class Compute(Thread):
    def __init__(self, request):
        Thread.__init__(self)
        self.request = request

    def run(self):
        print("start")
        time.sleep(5)
        print(self.request)
        print("done")



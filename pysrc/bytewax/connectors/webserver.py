"""
"""
import requests

from bytewax.outputs import DynamicOutput


class ServerOutput(DynamicOutput):
    def __init__(self, url):
        self.url = url

    def build(self):
        def send(value):
            requests.post(self.url, {"value": value})

        return send

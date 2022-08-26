import os
import time
class New:
    def __init__(self, date, title, body, tags, resource, url):
        self.pk = f"{int(time.time()*1000.0)}_{os.getpid()}"
        self.date = date
        self.title = title
        self.body = body
        self.tags = tags
        self.resource = resource
        self.url = url

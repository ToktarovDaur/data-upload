from json import load


def read_json(path):
    with open(path) as js:
        return load(js)
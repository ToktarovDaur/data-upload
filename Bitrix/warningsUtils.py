from warnings import filterwarnings


def ignore_warnings(action='ignore'):
    filterwarnings(action=action)
import os 

def get_package_dir(dirname):
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), dirname)
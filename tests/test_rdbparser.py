import os
import sys

# Add the parent directory of 'app' to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from app.main import *

stream = rdb_contents()

parser = RDBparser()

for s, k, v, e in parser.parse(stream):
    print(s, k, v, e)

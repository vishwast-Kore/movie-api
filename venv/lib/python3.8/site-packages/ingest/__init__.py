import os
import yaml

def lazy_run(fn, conds, *args, **kwargs):
    """Run a function only if any
    of the specified conditions return True"""
    if all(c() for c in conds):
        print('Running', fn.__name__)
        fn(*args, **kwargs)
    else:
        print('Skipping', fn.__name__)

def pipeline(segments):
    for fn, conds in segments:
        lazy_run(fn, conds)

def files_exist(files):
    return lambda: not all(os.path.exists(f) for f in files)

def from_spec(fname):
    spec = yaml.load(open(fname))
    for task in spec:
        print(task)
    print(spec)
import py_compile
from os.path import join, getsize, splitext
from os import walk, remove
import sys

for root, dirs, files in walk('.'):
    if '.svn' in dirs:
        dirs.remove('.svn')  # don't visit CVS directories
        pass
    for file_ in files:
        if splitext(join(root,file_))[1] == '.py':
            py_compile.compile(join(root,file_))
            pass
        pass
    pass

if len(sys.argv) > 1:
    if sys.argv[1] == 'remove':
        for root, dirs, files in walk('.'):
            if '.svn' in dirs:
                dirs.remove('.svn')  # don't visit CVS directories
                pass
            for file_ in files:
                if splitext(join(root,file_))[1] == '.py':
                    remove(join(root,file_))
                    pass
                pass
            pass
        pass
    pass


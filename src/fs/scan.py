# Copyright 2006 Uberan - All Rights Reserved

import sys
import FileSystem

fs = FileSystem()

for details in fs.list(sys.argv[1]):
    print details

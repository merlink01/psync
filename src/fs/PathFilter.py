# Copyright 2006 Uberan - All Rights Reserved

import fnmatch

class PathFilter(Record("patterns_to_drop", "paths_to_drop")):
    def __new__(cls, globs_to_drop):
        patterns_to_drop = compile_globs(globs)
        return cls.new(patterns_to_drop, set())

    def keep_path(self, path):
        if path in self.paths_to_drop:
            return False
        elif any(pattern.match(path) for pattern in self.patterns_to_drop):
            self.paths_to_drop.add(path)
            return False
        else:
            return True

def compile_globs(globs):
    return (re.compile(fnmatch.translate(glob), re.IGNORECASE)
            for glob in globs)

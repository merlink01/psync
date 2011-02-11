def compile_patterns(*patterns):
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)

TEMP_FILENAME_PATTERNS   = 
SYSTEM_FILENAME_PATTERNS = 

class FileFilter:
    def keep_path(self, path):
        return True

    def keep_stat(self, stat):
        return True

class HiddenFileFilter(FileFilter):
    def keep_stat(self, stat):
        return not stat.hidden

class SystemFileFilter(FileFilter):
    patterns = compile_patterns(r'thumbs\.db$', r'\.DS_Store')

    def keep_stat(self, stat):
        return (not stat.system and
                not any(bool(pattern.search(filename))
                        for pattern in self.patterns))

class TempFileFilter(FileFilter):
    #TODO: test these, r'^\.~.+\#$', r'^\#.+\#$')
    patterns = compile_patterns(r'~.*\.tmp$', r'~\$', r'.+~$')

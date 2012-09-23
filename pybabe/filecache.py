
import os


class FileCache(object):
    "Manage a file-based cache, with direction"

    tmpsuffix = ".343WRED.tmp"

    def __init__(self, cache_dir, size_limit):
        self.cache_dir = cache_dir
        self.size_limit = size_limit

    def lookup(self, name, fetch_callback):
        f = os.path.join(self.cache_dir, name)
        if os.path.exists(f):
            return f
        temp_filename = f + self.tmpsuffix
        fetch_callback(temp_filename)
        os.path.rename(temp_filename, f)
        self.cleanup()
        return f

    def cleanup(self):
        "Apply a global cleanup to the cache, trimming it to size_limit by removing oldest files first"
        global_size = 0
        for (dir, dir_names, file_names) in os.walk(self.cache_dir):
            for f in file_names:
                if f.endswith(self.tmpsuffix):
                    os.remove(f)
                file_stat = os.stat(f)
                file_size = file_stat.st_size
                last_modified = file_stat.st_last_modified
                global_size += file_size
        if global_size > self.size_limit:
            pass

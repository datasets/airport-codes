import os
import shutil

from .file_dumper import FileDumper


class PathDumper(FileDumper):

    def __init__(self, out_path='.', **options):
        super(PathDumper, self).__init__(options)
        self.out_path = out_path
        PathDumper.__makedirs(self.out_path)

    def write_file_to_output(self, filename, path):
        path = os.path.join(self.out_path, path)
        # Avoid rewriting existing files
        if self.add_filehash_to_path and os.path.exists(path):
            return
        path_part = os.path.dirname(path)
        PathDumper.__makedirs(path_part)
        shutil.copy(filename, path)
        return path

    @staticmethod
    def __makedirs(path):
        os.makedirs(path, exist_ok=True)

import zipfile

from .file_dumper import FileDumper


class ZipDumper(FileDumper):

    def __init__(self, out_file, **options):
        super(ZipDumper, self).__init__(options)
        self.out_file = open(out_file, 'wb')
        self.zip_file = zipfile.ZipFile(self.out_file, 'w')

    def write_file_to_output(self, filename, path):
        self.zip_file.write(filename, arcname=path,
                            compress_type=zipfile.ZIP_DEFLATED)

    def finalize(self):
        self.zip_file.close()
        if not self.out_file.closed:
            self.out_file.close()
        super(ZipDumper, self).finalize()

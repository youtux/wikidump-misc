import pathlib
import subprocess
import io
import gzip


def open_compressed_file(file_path):
    mode = 'rt'
    encoding = 'utf-8'

    if not isinstance(file_path, pathlib.Path):
        file_path = pathlib.Path(file_path)

    if file_path.suffix == '.7z':
        f = subprocess.Popen(
            ['7z', 'e', '-so', str(file_path)],
            stdout=subprocess.PIPE,
        )
        w = io.TextIOWrapper(f.stdout, encoding=encoding)
        return w
    elif file_path.suffix == '.gz':
        return gzip.open(str(file_path), mode, encoding=encoding)
    else:
        return file_path.open(mode, encoding=encoding)

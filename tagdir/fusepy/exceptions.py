import os


class FuseOSError(OSError):
    def __init__(self, errno):
        super(FuseOSError, self).__init__(errno, os.strerror(errno))

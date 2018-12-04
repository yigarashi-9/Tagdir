import logging


class DebugFilter(logging.Filter):
    def __init__(self) -> None:
        self.id = 1

    def filter(self, record):
        if record.levelno == logging.DEBUG:
            record.id = self.id
            self.id += 1
            return True
        else:
            return False


def tagdir_debug_handler() -> logging.Handler:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    debug_formatter = logging.Formatter(
        "%(levelname)s %(name)s (%(id)d): %(op)s "
        "%(path)s %(arguments)s %(message)s")
    ch.setFormatter(debug_formatter)
    ch.addFilter(DebugFilter())
    return ch

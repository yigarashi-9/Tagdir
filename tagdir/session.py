class Session:
    """
    This class is dummy for static analysis. This should be dynamically
    overridden by sessionmaker(bind=engine).
    """
    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

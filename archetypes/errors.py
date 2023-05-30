class MDPError(Exception):
    pass


class InvalidHeader(MDPError):
    """Raised when the header doesn't refer to a client or a worker"""
    pass


class WorkerNotConnected(MDPError):
    """Raised when the worker hasn't yet connected to the broker"""
    pass

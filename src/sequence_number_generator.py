def singleton(cls):
    instances = {}

    class SingletonWrapper(cls):
        def __new__(cls, *args, **kwargs):
            if cls not in instances:
                instances[cls] = super(SingletonWrapper, cls).__new__(cls)
                instances[cls].__init__(*args, **kwargs)
            return instances[cls]

        @classmethod
        def reset(cls):
            if cls in instances:
                del instances[cls]

    return SingletonWrapper


@singleton
class SequenceNumberGenerator:
    def __init__(self, start=0):
        self.current = start

    def __iter__(self):
        return self

    def __next__(self):
        current = self.current
        self.current += 1
        return current

    @classmethod
    def reset(cls):
        """Reset method stub for IDE compatibility."""
        raise NotImplementedError("This method is replaced by the singleton decorator.")

def singleton(singleton_cls):
    instances = {}

    class SingletonWrapper(singleton_cls):
        def __new__(cls, *args, **kwargs):
            if singleton_cls not in instances:
                # Store instance of the original class
                instances[singleton_cls] = super(SingletonWrapper, cls).__new__(singleton_cls)
                instances[singleton_cls].__init__(*args, **kwargs)
            return instances[singleton_cls]

        @classmethod
        def reset(cls):
            if singleton_cls in instances:
                del instances[singleton_cls]

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

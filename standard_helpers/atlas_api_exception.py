class AtlasAPIException(Exception):
    """
    Custom Altana Atlas API Exception
    """

    def __init__(self, message):
        """
        The constructor method of the Atlas API Exception

        Parameters
        ----------
        message : str
            The exception message
        """
        self.message = message
        super().__init__(self.message)

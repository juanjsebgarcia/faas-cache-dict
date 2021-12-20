class DataTooLarge(ValueError):
    """
    Raised if the data being added exceeds the dicts limit

    This being raised means that it was not possible to store the data within the
    user set max size constraint of the dict
    """

    pass

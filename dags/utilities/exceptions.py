class ApiError(Exception):
    """
    Exception raised for getting an error response from API.
    """
    def __init__(self, status_code):
        self.status_code = status_code

    def __str__(self):
        if self.status_code:
            if self.status_code == "400":
                error = "Missing required parameter."
            elif self.status_code == "401":
                error = "Unauthorized."
            elif self.status_code == "403":
                error = "Forbidden."
            elif self.status_code == "404":
                error = "Unable to find information."
            elif self.status_code == "500":
                error = "Internal error please try again."
            else:
                error = "Unknown error."
            msg = self.status_code + " error. " + error

        else:
            msg = "Unknown error."

        return msg

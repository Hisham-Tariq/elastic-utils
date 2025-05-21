
class ElasticUrls:
    """
        This class is used to generate elastic search endpoints
    """
    @classmethod
    def generate_url(cls, base_url: str, endpoint: str) -> str:
        """
        Generate the elastic search endpoint
        Args:
            base_url (str): The base URL
            endpoint (str): The endpoint
        Returns:
            str: The full URL
        """
        return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def __init__(self, index_name) -> None:
        self.__index_name = index_name
        self.add = self.generate_url(f"{self.__index_name}/_doc")
        self.search = self.generate_url(f"{self.__index_name}/_search")
        self.update_by_query = self.generate_url(f"{self.__index_name}/_update_by_query")
        self.delete_by_query = self.generate_url(f"{self.__index_name}/_delete_by_query")
        self.indicies =  self.generate_url("_aliases?pretty=true")

    def document(self, document_id) -> str:
        """
        Get the document endpoint which can be used to add, get, update or delete a document
        Args:
            document_id (str): The document id
        Returns:
            str: the document url
        """
        return self.generate_url(f"{self.__index_name}/_doc/{document_id}")
    
    def point_in_time_url(self, keep_alive: str) -> str:
        """
        Get the point in time url
        Args:
            keep_alive (str): The keep alive time i.e 1m 5m 10m 1h etc..
        Returns:
            str: The point in time url
        """
        return self.generate_url(f"{self.__index_name}/_search/point_in_time?keep_alive={keep_alive}")

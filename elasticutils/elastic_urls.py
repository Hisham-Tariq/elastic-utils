
class ElasticUrls:
    """
        This class is used to generate elastic search endpoints
    """
    @classmethod
    def generate_url(cls, endpoint: str, base_url: str) -> str:
        """
        Generate the elastic search endpoint
        Args:
            base_url (str): The base URL
            endpoint (str): The endpoint
        Returns:
            str: The full URL
        """
        return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def __init__(self, index_name, base_url) -> None:
        self.__index_name = index_name
        self.__base_url = base_url
        self.add = self.generate_url(f"{self.__index_name}/_doc", base_url)
        self.search = self.generate_url(f"{self.__index_name}/_search", base_url)
        self.update_by_query = self.generate_url(f"{self.__index_name}/_update_by_query", base_url)
        self.delete_by_query = self.generate_url(f"{self.__index_name}/_delete_by_query", base_url)
        self.indicies =  self.generate_url("_aliases?pretty=true", base_url)

    def document(self, document_id) -> str:
        """
        Get the document endpoint which can be used to add, get, update or delete a document
        Args:
            document_id (str): The document id
        Returns:
            str: the document url
        """
        return self.generate_url(f"{self.__index_name}/_doc/{document_id}", self.__base_url)
    
    def point_in_time_url(self, keep_alive: str) -> str:
        """
        Get the point in time url
        Args:
            keep_alive (str): The keep alive time i.e 1m 5m 10m 1h etc..
        Returns:
            str: The point in time url
        """
        return self.generate_url(f"{self.__index_name}/_search/point_in_time?keep_alive={keep_alive}", self.__base_url)

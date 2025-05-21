import base64
import requests
from typing import Dict, Any

class ElasticsearchClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        credentials = f"{username}:{password}"
        token = base64.b64encode(credentials.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }

    def get_document(self, index: str, doc_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/{index}/_doc/{doc_id}"
        response = requests.get(url, headers=self.headers, verify=False)
        return response.json()

    @classmethod
    def __send_request(cls, method, url, **kwargs) -> requests.Response:
        """
        Send a request to Elasticsearch.
        """
        # check if url is not a valid then attach the base url
        if not url.startswith("http"):
            url = f"{cls.base_url}/{url}"
        json = kwargs.get("data")
        request = None
        match method:
            case "get":
                request = requests.get
            case "post":
                request = requests.post
            case "put":
                request = requests.put
            case "delete":
                request = requests.delete
            case _:
                raise ValueError("Method not supported")

        return request(
            url=url,
            json=json,
            verify=False,
            headers=cls.headers,
        )

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Send a GET request to Elasticsearch.
        """
        return self.__send_request("get", endpoint, **kwargs)

    def post(self, endpoint: str, data: Dict[str, Any], **kwargs) -> requests.Response:
        """
        Send a POST request to Elasticsearch.
        """
        return self.__send_request("post", endpoint, data=data, **kwargs)

    def put(self, endpoint: str, data: Dict[str, Any], **kwargs) -> requests.Response:
        """
        Send a PUT request to Elasticsearch.
        """
        return self.__send_request("put", endpoint, data=data, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Send a DELETE request to Elasticsearch.
        """
        return self.__send_request("delete", endpoint, **kwargs)

    def search(self, index: str, query: Dict[str, Any], **kwargs) -> requests.Response:
        """
        Execute a search query on the specified index.
        
        Args:
            index: The name of the index to search
            query: The search query to execute
            
        Returns:
            requests.Response: The response from Elasticsearch
        """
        endpoint = f"{index}/_search"
        return self.post(endpoint, data=query, **kwargs)
        
    def index(self, index: str, document: Dict[str, Any], doc_id: str = None) -> requests.Response:
        """
        Index a document in Elasticsearch.
        
        Args:
            index: The name of the index
            document: The document to index
            doc_id: The document ID (optional)
            
        Returns:
            requests.Response: The response from Elasticsearch
        """
        if doc_id:
            endpoint = f"{index}/_doc/{doc_id}"
        else:
            endpoint = f"{index}/_doc"
        return self.post(endpoint, data=document)
        
    def update(self, index: str, doc_id: str, document: Dict[str, Any]) -> requests.Response:
        """
        Update a document in Elasticsearch.
        
        Args:
            index: The name of the index
            doc_id: The document ID
            document: The document fields to update
            
        Returns:
            requests.Response: The response from Elasticsearch
        """
        endpoint = f"{index}/_update/{doc_id}"
        return self.post(endpoint, data=document)
        
    def index_exists(self, index: str) -> bool:
        """
        Check if an index exists in Elasticsearch.
        
        Args:
            index: The name of the index
            
        Returns:
            bool: True if the index exists, False otherwise
        """
        try:
            response = self.get(f"{index}")
            return response.status_code == 200
        except Exception:
            return False
            
    def create_index(self, index: str, mapping: Dict[str, Any]) -> requests.Response:
        """
        Create an index in Elasticsearch with the specified mapping.
        
        Args:
            index: The name of the index
            mapping: The mapping for the index
            
        Returns:
            requests.Response: The response from Elasticsearch
        """
        body = {
            "mappings": mapping,
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            }
        }
        return self.put(f"{index}", data=body)
        
    def delete_index(self, index: str) -> requests.Response:
        """
        Delete an index from Elasticsearch.
        
        Args:
            index: The name of the index
            
        Returns:
            requests.Response: The response from Elasticsearch
        """
        return self.delete(f"{index}")

    @classmethod
    def create_point_in_time(
        cls, endpoint=None, index=None, base_url=None, keep_alive=None, **kwargs
    ):
        """
        This method creates a PIT for an Elasticsearch index, which allows for consistent search results 
        even if the index is being updated. You can either provide an endpoint directly or specify the 
        index and base URL.
        Args:
            endpoint (str, optional): The endpoint to create the PIT. If provided, it will be used directly.
            index (str, optional): The name of the Elasticsearch index for which to create the PIT.
            base_url (str, optional): The base URL of the Elasticsearch instance. If not provided, 
                                        the class-level base_url will be used.
            keep_alive (str, optional): The duration for which the PIT should be kept alive. Defaults to "1m".
            **kwargs: Additional keyword arguments.
        Returns:
            dict: The response from the Elasticsearch server.
        Raises:
            ValueError: If neither 'endpoint' nor 'index' and 'base_url' are provided.
        Example:
            # Using endpoint
            response = ElasticClient.create_point_in_time(endpoint="/my_custom_endpoint")
            # Using index
            response = ElasticClient.create_point_in_time(index="my_index")
            # Using index and base_url
            response = ElasticClient.create_point_in_time(index="my_index", base_url="http://localhost:9200")
        """
        if endpoint:
            return cls.__send_request("post", endpoint)
        # if any is none in index, base_url rais error
        if not index:
            raise ValueError(
                "Either 'endpoint' or 'index' and 'base_url'  must be provided"
            )
        base_url = base_url or cls.base_url

        keep_alive = keep_alive or "1m"
        url = f"{base_url}/{index}/_search/point_in_time?keep_alive={keep_alive}"
        return cls.__send_request("post", url)

    @classmethod
    def delete_point_in_time(cls, pit_id):
        """
        This method deletes a Point in Time (PIT) in Elasticsearch to release resources.
        
        Args:
            pit_id (str or list): The ID or list of IDs of the point in time to delete.
            
        Returns:
            dict: The response from the Elasticsearch server.
            
        Example:
            # Delete a single point in time
            response = ElasticsearchClient.delete_point_in_time(pit_id="my_pit_id")
            
            # Delete multiple points in time
            response = ElasticsearchClient.delete_point_in_time(pit_id=["pit_id_1", "pit_id_2"])
        """
        url = f"{cls.base_url}/_search/point_in_time"
        
        # Convert single pit_id to a list for consistent processing
        pit_ids = [pit_id] if isinstance(pit_id, str) else pit_id
        
        body = {"pit_id": pit_ids}
        return cls.__send_request("delete", url, data=body)

    @classmethod
    def search_with_pit(
        cls,
        pit_id: str,
        query,
        batch_size=10000,
        max_records=100000,
        fields=[],
        return_generator=False,
        initial_search_after=None,
    ):
        def generator():
            # yield results in batches with the search after
            total_fetched = 0
            search_after = initial_search_after

            while total_fetched < max_records:
                body = {
                    "track_total_hits": True,
                    "query": query,
                    "pit": {"id": pit_id, "keep_alive": "1m"},
                    "size": min(batch_size, max_records - total_fetched),
                    "sort": [{"timestamp": {"order": "desc"}}],
                }
                
                if fields:
                    body["_source"] = fields

                if search_after:
                    body["search_after"] = search_after

                response = cls.__send_request(
                    "post", f"{cls.base_url}/_search", data=body
                )
                if response.status_code != 200:
                    # import json
                    # print(json.dumps(body))
                    print(response.json())
                response.raise_for_status()
                data = response.json()

                hits = data.get("hits", {}).get("hits", [])
                if not hits:
                    break

                for hit in hits:
                    yield (hit["_source"], hits[-1]["sort"])

                total_fetched += len(hits)
                search_after = hits[-1]["sort"]

        if return_generator:
            return generator()

        results = []
        last_search_after = initial_search_after 
        for item, search_after in generator():
            last_search_after = search_after
            results.append(item)
        return results, last_search_after

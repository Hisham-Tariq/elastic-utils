import json
# QueryBuilderException
class QueryBuilderException(Exception):
    def __init__(self, message):
        self.message = message


class ElasticQueryBuilder:
    def __init__(self, index: str = "", track_total_hits: bool = True):
        self.index = index
        self.query = {"query": {}}
        self._aggs_count = 0
        self.current_query_type = None

    def __is_using_multi_conditional_query(self):
        return "bool" in self.query["query"].keys()

    def match(self, field: str, value):
        #  check if bool exist in query['query']
        KEY = "match"
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {KEY: {field: value}}
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = {field: value}
        return self

    def match_phrase(self, field: str, value):
        #  check if bool exist in query['query']
        KEY = "match_phrase"
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {KEY: {field: value}}
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = {field: value}
        return self

    def term(self, field: str, value):
        #  check if bool exist in query['query']
        KEY = "term"
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {KEY: {field: value}}
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = {field: {"value": value}}
        return self
    
    def terms(self, field: str, values):
        #  check if bool exist in query['query']
        KEY = "terms"
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {KEY: {field: values}}
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = {field: values}
        return self
    
    def query_string(self, field: str, value):
        #  check if bool exist in query['query']
        KEY = "query_string"
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {
                    KEY: {
                        "default_field": field,
                        "query": value
                    }
                }
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = {
                        "default_field": field,
                        "query": value
                    }
        return self

    def range(self, field: str, gte=None, lte=None):
        #  check if bool exist in query['query']
        KEY = "range"
        data = {field: {}}
        if gte is not None:
            data[field]["gte"] = gte
        if lte is not None:
            data[field]["lte"] = lte
        if gte is None and lte is None:
            raise QueryBuilderException("Must provide either gte or lte")
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {KEY: data}
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = data
        return self
    
    def exists(self, field: str):
        #  check if bool exist in query['query']
        KEY = "exists"
        if self.__is_using_multi_conditional_query():
            self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
                {KEY: {"field": field}}
            )
        else:
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(f"Cannot use {KEY} query")
            self.query["query"][KEY] = {"field": field}
        return self

    def add_bool(self, sub_query):
        if not isinstance(sub_query, ElasticQueryBuilder):
            raise QueryBuilderException("sub_query must be an instance of ElasticQueryBuilder")
        # check if multi conditional query is being used else raise error
        if not self.__is_using_multi_conditional_query():
            raise QueryBuilderException("Cannot add bool query")
        query = sub_query.build()
        if not query:
            raise QueryBuilderException("sub_query must contain query")
        if "bool" not in query["query"].keys():
            raise QueryBuilderException("sub_query must contain bool query")
        self.query["query"]["bool"].setdefault(self.current_query_type, []).append(
            query["query"]
        )
        return self

    def __setup_multi_conditional_query(self, query_type: str):
        if not self.__is_using_multi_conditional_query():
            # if any other key exists in query['query'] then raise error
            if len(self.query["query"].keys()) > 0:
                raise QueryBuilderException(
                    f"Cannot use {query_type} conditional query"
                )
            self.query["query"]["bool"] = {}

    def should(self):
        self.__setup_multi_conditional_query("should")
        self.current_query_type = "should"
        return self
    
    def filter(self):
        self.__setup_multi_conditional_query("filter")
        self.current_query_type = "filter"
        return self

    def must(self):
        self.__setup_multi_conditional_query("must")
        self.current_query_type = "must"
        return self

    def must_not(self):
        self.__setup_multi_conditional_query("must_not")
        self.current_query_type = "must_not"
        return self

    def query_size(self, size: int):
        self.query["size"] = size
        return self

    def from_size(self, from_value: int, size: int):
        """
        Sets the from and size parameters in the query.
        
        Args:
            from_value: The starting position of the results
            size: The number of results to return
            
        Returns:
            self: The ElasticQueryBuilder instance
        """
        self.query["from"] = from_value
        self.query["size"] = size
        return self

    def source(self, *source):
        if len(source) > 0:
            self.query["_source"] = source
        return self

    def add_script(self, script: dict):
        self.query["script"] = script
        return self

    def __aggs_by_type(self, field: str, *args, **kwargs):
        def get(key: str, default):
            value = kwargs.get(key, default)
            if key == "sort" and value not in ["desc", "asc"]:
                raise QueryBuilderException("sort must be either desc or asc")
            if key == "aggs_type" and value not in ["terms", "date_histogram", "top_hits", "cardinality"]:
                raise QueryBuilderException("aggs_type must be either terms, date_histogram, top_hits, or cardinality")
            if key == "sort_on" and value not in ['_count', '_key']:
                raise QueryBuilderException("sort_on must be either _count or _key")
            return value

        aggs_type = get("aggs_type", "terms")
        if aggs_type == "terms":
            return {"terms": {"field": field, "order": {get('sort_on', '_count'): get("sort", "desc")}, "size": get("size", 10)}}
        elif aggs_type == "date_histogram":
            return {"date_histogram": {"field": field, "fixed_interval": get("fixed_interval", "1d")}}
        elif aggs_type == "top_hits":
            return {"top_hits": {"size": get("size", 1)}}
        elif aggs_type == "cardinality":
            return {"cardinality": {"field": field}}

    def aggs(self, field: str, *args, **kwargs):
        """
        Adds an aggregation to the query.

        Args:
            field (str): The field to aggregate on.
            *args: Optional arguments. The first argument can be used to specify the size of the aggregation.
            **kwargs: Optional keyword arguments. Additional parameters for the aggregation.
            - size (int): The size of the aggregation.
            - sort (str): The sort order of the aggregation. Must be either "desc" or "asc".
            - sort_on (str): The field to sort on. default is _count. Must be either "_count" or "_key".
            - aggs_type (str): The type of aggregation. Must be either "terms" or "date_histogram".
            - fixed_interval (str): The fixed interval for the date_histogram aggregation.

        Returns:
            self: The QueryBuilder instance.

        """
        if len(args) > 0 and "size" not in kwargs.keys():
            kwargs['size'] = args[0]
        if len(args) > 1 and "sort" not in kwargs.keys():
            kwargs['sort'] = args[1]
            
        agg = {
            "data": self.__aggs_by_type(field, **kwargs)
        }
        if self._aggs_count == 0:
            self.query["aggs"] = agg
        else:
            # find the last aggs
            parent = self.query
            for _ in range(self._aggs_count):
                parent = parent["aggs"]["data"]
            parent["aggs"] = agg
        self._aggs_count += 1
        return self
    
    def track_total_hits(self, track_total_hits: bool = True):
        self.query["track_total_hits"] = track_total_hits
        return self
    
    def minimum_should_match(self, minimum_should_match: int):
        if not self.__is_using_multi_conditional_query():
            raise QueryBuilderException("Cannot add minimum_should_match query")
        self.query["query"]["bool"]["minimum_should_match"] = minimum_should_match
        return self

    def sort(self, field: str, order: str = "desc"):
        if order not in ["desc", "asc"]:
            raise QueryBuilderException("order must be either desc or asc")
        self.query.setdefault("sort", []).append({field: {"order": order}})
        return self

    def print(self):
        print(json.dumps(self.query))
        return self

    def build(self):
        # check if query is empty
        if not self.query["query"]:
            self.query.pop("query")
        return self.query
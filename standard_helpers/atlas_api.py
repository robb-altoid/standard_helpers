import pycountry
import requests
import traceback


####### from atlas_api_constants.py ########
class AtlasAPIConstants:
    """
    The Altana Atlas API Constants
    """

    # Node Types
    COMPANY_NODE = "company"
    FACILITY_NODE = "facility"
    ADDRESS_NODE = "address"
    DENIED_PARTY_NODE = "denied_party"
    NODE_TYPES = [COMPANY_NODE, FACILITY_NODE, ADDRESS_NODE, DENIED_PARTY_NODE]

    # Edge Types
    COMPANY_SENDS_TO_EDGE = "company_sends_to"
    COMPANY_SENDS_TO_SUMMARIZED_EDGE = "company_sends_to_summarized"
    FACILITY_SENDS_TO_EDGE = "facility_sends_to"
    FACILITY_SENDS_TO_SUMMARIZED_EDGE = "facility_sends_to_summarized"
    OPERATED_BY_EDGE = "operated_by"
    LOCATED_AT_EDGE = "located_at"
    OWNED_BY_EDGE = "owned_by"
    DENIED_PARTY_RELATED_TO_EDGE = "denied_party_related_to"
    EDGE_TYPES = [
        COMPANY_SENDS_TO_EDGE,
        COMPANY_SENDS_TO_SUMMARIZED_EDGE,
        FACILITY_SENDS_TO_EDGE,
        FACILITY_SENDS_TO_SUMMARIZED_EDGE,
        OPERATED_BY_EDGE,
        LOCATED_AT_EDGE,
        OWNED_BY_EDGE,
        DENIED_PARTY_RELATED_TO_EDGE,
    ]

    # Edge Traversals
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    ANY = "any"
    EDGE_TRAVERSALS = [INBOUND, OUTBOUND, ANY]

    # Miscellaneous
    WILDCARD_EDGE_ID = "-"


####### from atlas_api_exceptions.py ########
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


####### from base_atlas_api.py ########
# TODO: wire this up to packaging
VERSION = "0.0.1"


class BaseAtlasAPI:
    """
    The Base Altana Atlas API SDK

    Attributes
    ----------
    ISO2_COUNTRIES : set
        A list of possible 2 letter ISO2 codes for all countries
    PAGINATION_MAXIMUM : int
        The current maximum page number supported for pagination within the Altana Atlas API
    api_key : str
        The API key provided by Altana for the API service
    api_host : str
        The hostname for the API service that you will be hitting
    headers : dict
        The dictionary representing the headers for the API requests
    """

    # TODO: drop the dep and just grab the json from ?
    # https://github.com/flyingcircusio/pycountry/blob/master/src/pycountry/databases/iso3166-1.json
    ISO2_COUNTRIES = set(c.alpha_2 for c in list(pycountry.countries))

    PAGINATION_MAXIMUM = 100

    def __init__(self, api_key: str, api_host_name: str):
        """
        The constructor method of the Atlas API

        Parameters
        ----------
        api_key : str
            The API key provided by Altana for the API service
        api_host : str
            The hostname for the API service that you will be hitting
        """
        self.api_key = api_key
        self.host_name = api_host_name.rstrip("/")
        self._session = requests.Session()
        headers = {
            "User-Agent": f"atlas-foundry-client (v{VERSION})",
            "X-Api-Key": self.api_key,
        }
        self._session.headers.update(headers)
        self.headers = headers

    def _make_get_request(self, route, **kwargs):
        route = route.lstrip("/")
        full_url = f"{self.host_name}/{route}"
        response = self._session.get(full_url, **kwargs)
        if response.status_code == 200:
            return response.json()
        raise AtlasAPIException(response.json())

    def _check_node_type(self, node_type):
        if node_type not in AtlasAPIConstants.NODE_TYPES:
            expected_str = ", ".join(sorted(AtlasAPIConstants.NODE_TYPES))
            raise AtlasAPIException(
                f"Unknown Node Type: Given {node_type}, expected {expected_str}"
            )

    # ---------------------------------------------- NODE ENDPOINTS ----------------------------------------------
    def node(self, node_type: str, page: int = None):
        """
        Get a list of node objects

        Parameters
        ----------
        node_type : str
            The type of node being requested
        page : int, optional
            Page number to return from results

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        self._check_node_type(node_type)

        # TODO: Page Param Validation?
        if page is not None:
            params["page"] = page

        return self._make_get_request(f"node/{node_type}", params=params)

    def describe_node(self, node_type: str):
        """
        Returns a description of the fields

        Parameters
        ----------
        node_type : str
            The type of node being requested. Available values : company, facility, address, denied_party

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        self._check_node_type(node_type)
        return self._make_get_request(f"node/{node_type}/describe")

    def get_node_by_id(self, node_type: str, node_id: str):
        """
        Get a node by its ID.

        Parameters
        ----------
        node_type : str
            The type of node being requested. Available values : company, facility, address, denied_party
        node_id : str
            The ID of the node

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        self._check_node_type(node_type)
        return self._make_get_request(f"node/{node_type}/id/{node_id}")

    def find_the_shortest_path_between_nodes(
        self,
        node_type: str,
        node_id: str,
        destination_node_type: str,
        destination_node_id: str,
    ):
        """
        Find the shortest path between two nodes, if it exists.

        Parameters
        ----------
        node_type : str
            The type of node being requested. Available values : company, facility, address, denied_party
        node_id : str
            The ID of the node
        destination_node_type : str
            The type of the destination node. Available values : company, facility, address, denied_party
        destination_node_id : str
            The ID of the destination node

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        self._check_node_type(node_type)

        # left as a separate check because the error msg is different
        if destination_node_type not in AtlasAPIConstants.NODE_TYPES:
            raise AtlasAPIException(
                f"Unknown Destination Node Type: Given {destination_node_type}, expected {', '.join(AtlasAPIConstants.NODE_TYPES)}"
            )

        return self._make_get_request(
            f"node/{node_type}/id/{node_id}/shortest_path/{destination_node_type}/id/{destination_node_id}"
        )

    def traverse_across_edges(
        self,
        node_type: str,
        node_id: str,
        edge_type: str,
        edge_direction: str,
        min_query_depth: int = 1,
        max_query_depth: int = 1,
        page: int = None,
    ):
        """
        Traverse across a given edge type in the given direction, starting at the given node. Maximum number of hops allowed between nodes is 3.

        Parameters
        ----------
        node_type : str
            The type of node being requested. Available values : company, facility, address, denied_party
        node_id : str
            The ID of the node
        edge_type : str
            The type of edge being requested. Available values : company_sends_to, company_sends_to_summarized, facility_sends_to, facility_sends_to_summarized, operated_by, located_at, owned_by, denied_party_related_to
        edge_direction : str
            The direction to traverse across the edge. Available values : inbound, outbound, any
        min_query_depth : int, optional
            The minimum number of hops returned by the query. Defaults to 1. Must be equal or less than max_query_depth
        max_query_depth : int, optional
            The maximum number of hops returned by the query. Defaults to 1. Must be equal or greater than min_query_depth
        page : int, optional
            Page number to return from results

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        if node_type not in AtlasAPIConstants.NODE_TYPES:
            raise AtlasAPIException(
                f"Unknown Node Type: Given {node_type}, expected {', '.join(AtlasAPIConstants.NODE_TYPES)}"
            )

        if edge_type not in AtlasAPIConstants.EDGE_TYPES:
            raise AtlasAPIException(
                f"Unknown Edge Type: Given {edge_type}, expected {', '.join(AtlasAPIConstants.EDGE_TYPES)}"
            )

        if max_query_depth > 3:
            raise AtlasAPIException(
                f"Invalid Max Query Depth: Given {max_query_depth}, expected a number less than 3. 3 is the maximum allowed number of hops"
            )

        if min_query_depth < 1:
            raise AtlasAPIException(
                f"Invalid Min Query Depth: Given {min_query_depth}, min query depth cannot be less than 1"
            )

        if min_query_depth > max_query_depth:
            raise AtlasAPIException(
                f"Invalid Min and Max Query Depth: Given Min ({min_query_depth}) & Max ({max_query_depth}) query depth, max query depth must be greater than or equal to min query depth."
            )

        # TODO: Page Param Validation?

        params["min_query_depth"] = min_query_depth
        params["max_query_depth"] = max_query_depth
        if page_number is not None:
            params["page"] = page

        return self._make_get_request(
            f"node/{node_type}/id/{node_id}/traverse/{edge_type}/direction/{edge_direction}",
            params=params,
        )

    # ---------------------------------------------- EDGE ENDPOINTS ----------------------------------------------
    def describe_edge(self, edge_type: str):
        """
        Returns a description of the fields

        Parameters
        ----------
        edge_type : str
            The type of edge being requested. Available values : company_sends_to, company_sends_to_summarized, facility_sends_to, facility_sends_to_summarized, operated_by, located_at, owned_by, denied_party_related_to

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        if edge_type not in AtlasAPIConstants.EDGE_TYPES:
            raise AtlasAPIException(
                f"Unknown Edge Type: Given {edge_type}, expected {', '.join(AtlasAPIConstants.EDGE_TYPES)}"
            )

        return self._make_get_request(f"edge/{edge_type}/describe")

    def get_edge_by_id(self, edge_type: str, edge_id: str):
        """
        Get an edge by its ID

        Parameters
        ----------
        edge_type : str
            The type of edge being requested. Available values : company_sends_to, company_sends_to_summarized, facility_sends_to, facility_sends_to_summarized, operated_by, located_at, owned_by, denied_party_related_to
        edge_id : str
            The ID of the edge

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        if edge_type not in AtlasAPIConstants.EDGE_TYPES:
            raise AtlasAPIException(
                f"Unknown Edge Type: Given {edge_type}, expected {', '.join(AtlasAPIConstants.EDGE_TYPES)}"
            )

        return self._make_get_request(f"edge/{edge_type}/id/{edge_id}")

    def get_edges(
        self, edge_type: str, source_id: str, destination_id: str, page: int = None
    ):
        """
        Get an edge by its source or destination ID

        :param edge_type: The type of edge being requested (e.g. company_sends_to, company_sends_to_summarized, facility_sends_to, facility_sends_to_summarized,
            operated_by, located_at, owned_by, denied_party_related_to)
        :param source_id: The source ID of the edge or "-" for any source ID
        :param destination_id: The destination ID of the edge or "-" for any destination ID
        :param page: Page number to return from results (Zero based indexing)
        :return A JSON object of the requested edges or throws an exception if an error occurs

        Parameters
        ----------
        edge_type : str
            The type of edge being requested. Available values : company_sends_to, company_sends_to_summarized, facility_sends_to, facility_sends_to_summarized, operated_by, located_at, owned_by, denied_party_related_to
        source_id : str
            The source ID of the edge or "-" for any source ID
        destination_id : str
            The destination ID of the edge or "-" for any destination ID
        page : int, optional
            Page number to return from results

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        if edge_type not in AtlasAPIConstants.EDGE_TYPES:
            raise AtlasAPIException(
                f"Unknown Edge Type: Given {edge_type}, expected {', '.join(AtlasAPIConstants.EDGE_TYPES)}"
            )

        if source_id == "-" and destination_id == "-":
            raise AtlasAPIException("Both source_id and destination_id cannot be '-'")

        if page is not None:
            params["page"] = page

        return self._make_get_request(
            f"{self.host_name}/edge/{edge_type}/source/{source_id}/destination/{destination_id}",
            params=params,
        )

    # ---------------------------------------------- SEARCH ENDPOINTS ----------------------------------------------
    def search_company(self, company_name: str, page: int = 0):
        """
        The Company Search endpoint allows users to search for companies by name. The endpoint returns the Altana IDs for companies matching that name, ordered by search relevance,
        as well as information on the company including: the products it trades, its industries, its buyers and suppliers, the risks and restrictions (i.e., sanctions, export controls, etc.)
        associated with the company, and the volume of trade associated with the company (via "number_records").

        Parameters
        ----------
        company_name : str
            A company name, variant, identifier, or query term
        page : int, optional
            Page number to return from results (0-99)

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        # Page Number Input Validation
        if page < 0 or page > 99:
            raise AtlasAPIException(
                f"Invalid Page Number: provided page_number, {page_number}, must be between 0-99"
            )

        params["page"] = page
        params["company_name"] = company_name

        return self._make_get_request(f"search/company", params=params)

    def post_search_company(self):
        # TODO: What is this endpoint
        raise NotImplementedError("post_search_company not yet implemented")

    def search_facility(self, full_address: str, company_name: str, page: int = 0):
        """
        Facility Search.

        Parameters
        ----------
        full_address : str
            The full address to search for
        company_name : str
            The company name to search for
        page : int, optional
            The Page number to return from results (0-99)

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        params["full_address"] = full_address
        params["company_name"] = company_name

        # Page Number Input Validation
        if page < 0 or page > 99:
            raise AtlasAPIException(
                f"Invalid Page Number: provided page_number, {page_number}, must be between 0-99"
            )

        params["page"] = page

        return self._make_get_request(f"search/facility", params=params)

    def post_search_facility(self):
        # TODO: What is this endpoint
        raise NotImplementedError("TODO: implement post_search_facility")

    def search_hs_code(self, search_string: str, page: int = 0):
        """
        Find HS Codes and associated descriptions

        Parameters
        ----------
        search_string : str
            Either an HS Code prefix or a free text string to search for in the descriptions
        page : int, optional
            Page number to return from results

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        params["search_string"] = search_string

        # TODO: Page Number Input Validation?
        # if page < 0 or page > 99:
        #  raise AtlasAPIException(f"Invalid Page Number: provided page_number, {page_number}, must be between 0-99")
        params["page"] = page
        return self._make_get_request(f"search/hs_code", params=params)

    def search_nace_code(self, search_string: str, page: int = 0):
        """
        Find Industry codes (NACE codes) and associated descriptions

        Parameters
        ----------
        search_string : str
            Either a NACE code prefix or a free text string to search for in the descriptions
        page : int, optional
            Page number to return from results

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        params["search_string"] = search_string

        # TODO: Page Number Input Validation?
        # if page < 0 or page > 99:
        #  raise AtlasAPIException(f"Invalid Page Number: provided page_number, {page_number}, must be between 0-99")

        params["page"] = page

        return self._make_get_request(f"search/hs_code", params=params)

    # ---------------------------------------------- MATCH ENDPOINTS ----------------------------------------------
    def match_company(self, company_name: str):
        """
        The Company Match Endpoint.

        Parameters
        ----------
        company_name : str
            A company name

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        return self._make_get_request(
            f"match/company", params={"company_name": company_name}
        )

    def match_facility(self, company_name: str, full_address: str):
        """
        Facility Match.

        Parameters
        ----------
        company_name : str
            The company name to search for
        full_address : bool, optional
            The full address or valid GeoJson Polygon to search for

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        params["full_address"] = full_address
        params["company_name"] = company_name

        r = requests.get(
            f"{self.host_name.replace('v2', 'v1')}/facility/match/",
            headers=self.headers,
            params=params,
        )
        if r.status_code == 200:
            return r.json()
        # TODO: does it make sense to raise on all non-200 codes?
        # TODO: handle non-json responses?
        raise AtlasAPIException(r.json())

    # ---------------------------------------------- VALUE CHAIN ENDPOINTS ----------------------------------------------
    # TODO
    # ---------------------------------------------- BULK ENDPOINTS ----------------------------------------------
    def get_bulk_edges_by_type(self, edge_type: str, edge_ids: list):
        """
        Send a list of edge IDs and receive back a list of edge objects that match

        Parameters
        ----------
        edge_type : str
            The type of edge being requested. Available values : company_sends_to, company_sends_to_summarized, facility_sends_to, facility_sends_to_summarized, operated_by, located_at, owned_by, denied_party_related_to
        edge_ids : list
            a list of IDs

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        if edge_type not in AtlasAPIConstants.EDGE_TYPES:
            raise AtlasAPIException(
                f"Unknown Edge Type: Given {edge_type}, expected {', '.join(AtlasAPIConstants.EDGE_TYPES)}"
            )

        params["request_ids"] = edge_ids

        r = requests.post(
            f"{self.host_name}/bulk/edge/{edge_type}",
            headers=self.headers,
            params=params,
        )
        if r.status_code == 200:
            return r.json()
        else:
            raise AtlasAPIException(r.json())

    def get_bulk_hs_codes(self, hs_codes: list):
        """
        Send a list of HS Codes and receive back a list of HS Codes that match

        Parameters
        ----------
        hs_codes : list
            a list of IDs

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        params["request_ids"] = hs_codes

        r = self._session.post(f"{self.host_name}/bulk/hs_code", params=params)
        if r.status_code == 200:
            return r.json()
        else:
            raise AtlasAPIException(r.json())

    def get_bulk_nodes_by_type(self, node_type: str, node_ids: list):
        """
        Send a list of edge IDs and receive back a list of edge objects that match

        Parameters
        ----------
        edge_type : str
            The type of node being requested. Available values : company, facility, address, denied_party
        node_ids : list
            a list of IDs

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        params = {}

        if node_type not in AtlasAPIConstants.NODE_TYPES:
            raise AtlasAPIException(
                f"Unknown Node Type: Given {node_type}, expected {', '.join(AtlasAPIConstants.NODE_TYPES)}"
            )

        params["request_ids"] = node_ids

        r = requests.post(
            f"{self.host_name}/bulk/node/{node_type}",
            headers=self.headers,
            params=params,
        )
        if r.status_code == 200:
            return r.json()
        else:
            raise AtlasAPIException(r.json())

    # ---------------------------------------------- DEFAULT ENDPOINTS ----------------------------------------------
    def get_atlas_graph_types(self):
        """
        Get node and edge types of the knowledge graph

        Returns
        -------
        json
            a json object representing the response of the API call

        Raises
        ------
        AtlasAPIException
            If the API request status code is not 200
        """
        r = requests.get(f"{self.host_name}/types", headers=self.headers)
        if r.status_code == 200:
            return r.json()
        else:
            raise AtlasAPIException(r.json())



####### from atlas_api.py ########
class AtlasAPI(BaseAtlasAPI):
    """
    The Altana Atlas API SDK
    """

    def __init__(self, api_key: str, api_host_name: str):
        """
        The constructor method of the Atlas API

        Parameters
        ----------
        api_key : str
            The API key provided by Altana for the API service
        api_host : str
            The hostname for the API service that you will be hitting
        """
        super().__init__(api_key, api_host_name)

    def get_all_company_search_results(self, query: str, max_results=None):
        """
        Paginates through all search results for the inputted query and returns a list of Company objects.
        Currently, the API only supports pagination through up to 100 pages (0-99).

        Parameters
        ----------
        query : str
            The company name to query the Altana Atlas for
        max_results : int, optional
            The maximum number of search results you would like returned

        Returns
        -------
        list
            A list of company objects returned by the company search within the Atlana Atlas for the specified query
        """
        results = []
        page_num, page_size, num_results = 0, 100, 1000
        while (
            (page_num - 1) * page_size
        ) < num_results and page_num < self.PAGINATION_MAXIMUM:
            try:
                api_response = self.search_company(query, page=page_num)
                results.extend([x["node"] for x in api_response["search_results"]])
                page_num, page_size, num_results = (
                    page_num + 1,
                    api_response.page_size,
                    api_response.num_results,
                )
                if max_results is not None and len(results) >= max_results:
                    results = results[:max_results]
                    break
            except Exception:
                print(traceback.format_exc())
                break
        return results



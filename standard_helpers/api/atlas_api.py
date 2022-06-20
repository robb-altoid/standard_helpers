from utils.api.base_atlas_api import BaseAtlasAPI
import traceback


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

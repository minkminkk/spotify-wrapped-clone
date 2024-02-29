from typing import List
from queue import Queue
from collections import defaultdict
import logging
import requests
from spotify_api_client.checks import check_response_json


class APISession:
    """Abstraction for Spotify WebAPI interaction. Includes client token
    retrieval using client credentials upon instantiation.
    
    Should use with `with` block to close HTTP session automatically upon
    exiting. Avoid leaving the session hanging when unhandled exceptions occur.

    Usage:
        from auth import ClientAuthenticator, ClientCredentialsStrategy
        auth = ClientAuthenticator(<CLIENT_ID>, <CLIENT_SECRET>)
        session = APISession(auth.get_access_token())
    """

    def __init__(self, access_token: dict):
        if not isinstance(access_token, dict):
            error_msg = "access_token type expected to be dict, " \
                + f"got {type(access_token).__name__} instead"
            raise ValueError(error_msg)

        # Root URL
        self.root_url = "https://api.spotify.com/v1"

        # Create new HTTP session when initialized
        self.http_session = requests.Session()

        # Update authorization header for each request onwards
        self.http_session.headers.update(
            {
                "Authorization": \
                    access_token["token_type"] \
                        + " " \
                        + access_token["access_token"]
            }
        )


    def close(self):
        # Close and delete HTTP session. Delete client token
        self.http_session.close()
        del self.http_session


    def __enter__(self):
        return self


    def __exit__(self, *args):
        self.close()


    def get_genres(self) -> dict:
        """Get all available genres in Spotify API.

        :return: response of all available genres
        """

        url = self.root_url + "/recommendations/available-genre-seeds"
        response = self.http_session.get(url)

        response.raise_for_status()
        check_response_json(response)

        r_json = response.json()
        if "genres" not in r_json.keys():
            logging.error("Unexpected JSON response schema")
            raise Exception("Invalid response")

        return r_json
    

    def search_items(
        self, 
        q: str, 
        type: str | List[str],
        market: str | None = None, 
        limit: int | None = None,
        offset: int | None = None,
        include_external: str | None = None,
        recursive: bool = False,
        max_pages: int = 5
    ) -> dict:
        """Query items (albums, tracks, artists, playlists...).

        :param q: query string, defined [here](https://developer.spotify.com/documentation/web-api/reference/search)
        :param type: item types to search
        :param market: country code of market, defaults to None
        :param limit: maximum number of results to return in each item type,
            defaults to None
        :param offset: offset from first result, defaults to None
        :param include_external: include external audio, defaults to None
        :param recursive: whether to follow URLs in `next` tags, defaults to False
        :param max_pages: if recursive is True, follow until max_pages retrieved
        :return: response containing query results, stripped unrelated metadata
            (limit, offset, total, next URLs...)
        """

        url = self.root_url + "/search"
        params = {
            "q": q,
            "type": type,
            "market": market,
            "limit": limit,
            "offset": offset,
            "include_external": include_external
        }

        # 2 different implementations based on `recursive` arg
        res = defaultdict(list)
        if not recursive:
            response = self.http_session.get(url, params = params)

            response.raise_for_status()
            check_response_json(response)

            r_json = response.json()
            for key in r_json.keys():
                res[key].extend(r_json[key]["items"])
        else:   # follow next URLs in response
            # NOTE: this implementation is very hard to test because total amount
            # of items returned from Spotify changes through each request.
            # Maybe it is better to find more testable approaches in the future.
            page_count = 0
            prep_req = requests.Request("GET", url, params = params).prepare()
                # construct first URL from base URL and params

            # Put URLs to waiting queue - done when queue is empty
            q_urls = Queue()
            q_urls.put_nowait(prep_req.url)

            # Fetch data until queue empty 
            # or reach max number of pages, if there is
            while not q_urls.empty() and page_count < max_pages:
                url = q_urls.get_nowait()

                logging.info("Fetching data from " + url)
                response = self.http_session.get(url)
                page_count += 1

                response.raise_for_status()
                check_response_json(response)

                r_json = response.json()
                for key in r_json.keys():
                    next_url = r_json[key]["next"]
                    if next_url:
                        q_urls.put_nowait(next_url)
                    res[key].extend(r_json[key]["items"])

            return res


    def get_tracks(
        self,
        track_id: str | List[str],
        market: str | None = None
    ) -> dict:
        """Query information about tracks.

        :param track_ids: `track_id` or list of `track_id`
        :type track_ids: str | List[str]
        :param market: country code of market, defaults to None
        :type market: str | None, optional
        :return: response containing query results
        :rtype: dict
        """

        url = self.root_url + "/tracks"
        params = {
            "ids": ",".join(track_id) \
                if isinstance(track_id, List) else track_id,
            market: market
        }
        response = self.http_session.get(url, params = params)

        response.raise_for_status()
        check_response_json(response)

        return response.json()
    
    # The remaining endpoints are not implemented 
    # due to not needed in this project
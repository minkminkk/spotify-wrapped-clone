from typing import List
import base64
import os
from queue import Queue
from collections import defaultdict

import requests
from utils.spotify_api_client.checks import check_response_json, check_status_code

ACCOUNTS_ROOT_URL = "https://accounts.spotify.com/api/token"
API_ROOT_URL = "https://api.spotify.com/v1"


class SpotifyAPISession:
    """Abstraction for Spotify WebAPI interaction. Includes client token
    retrieval using client credentials upon instantiation.
    
    Should use with `with` block to close HTTP session automatically upon
    exiting. Avoid leaving the session hanging when unhandled exceptions occur.
    """

    def __init__(self, client_id: str = None, client_secret: str = None):
        # Validate inputs
        if client_id:
            self.__client_id = client_id
        else:
            self.__client_id = os.getenv("SPOTIFY_CLIENT_ID")
            if not self.__client_id:
                raise EnvironmentError("SPOTIFY_CLIENT_ID not found.")

        if client_secret:
            self.__client_secret = client_secret
        else:
            self.__client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
            if not self.__client_secret:
                raise EnvironmentError("SPOTIFY_CLIENT_SECRET not found.")

        # Create new HTTP session when initialized
        self.http_session = requests.Session()

        # Get client token via client credentials
        self.client_token = self._get_credentials()

        # Update authorization header for each request onwards
        self.http_session.headers.update(
            {
                "Authorization": \
                    self.client_token["token_type"] \
                        + " " \
                        + self.client_token["access_token"]
            }
        )


    def close(self):
        # Close and delete HTTP session. Delete client token 
        self.http_session.close()
        del self.http_session
        del self.client_token


    def __enter__(self):
        return self


    def __exit__(self, *args):
        self.close()


    def _get_credentials(self) -> dict:
        """Get client credentials via Spotify API.

        :return: an access token with information (id, type, valid time)
        :rtype: dict
        """
        # Prepare auth msg in header in base64 (as specified in API docs)
        auth_str = f"{self.__client_id}:{self.__client_secret}"
        auth_bytes = auth_str.encode("utf-8")
        auth_b64 = base64.b64encode(auth_bytes)
        auth_msg = auth_b64.decode("utf-8")

        response = self.http_session.post(
            url = ACCOUNTS_ROOT_URL, 
            data = {"grant_type": "client_credentials"},
            headers = {
                "Authorization": f"Basic {auth_msg}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )

        check_response_json(response)
        check_status_code(response)

        return response.json()


    def get_genres(self) -> dict:
        """Get all available genres in Spotify API.

        :raises Exception: cannot parse JSON or unexpected JSON schema
        :return: response of all available genres
        :rtype: dict
        """

        url = API_ROOT_URL + "/recommendations/available-genre-seeds"
        response = self.http_session.get(url)

        check_response_json(response)
        check_status_code(response)

        return response.json()
    

    def search_items(
        self, 
        q: str, 
        type: str | List[str],
        market: str | None = None, 
        limit: int = None,
        offset: int = None,
        include_external: str | None = None,
        recursive: bool = False
    ) -> dict:
        """Query items (albums, tracks, artists, playlists...).

        :param q: query string, defined [here](https://developer.spotify.com/documentation/web-api/reference/search)
        :type q: str
        :param type: item types to search
        :type type: str | List[str]
        :param market: country code of market, defaults to None
        :type market: str | None, optional
        :param limit: maximum number of results to return in each item type,
            defaults to None
        :type limit: int, optional
        :param offset: offset from first result, defaults to None
        :type offset: int, optional
        :param include_external: include external audio, defaults to None
        :type include_external: str | None, optional
        :param recursive: whether to follow URLs in `next` tags, defaults to False
        :type recursive: bool, optional
        :return: response containing query results, stripped unrelated metadata
            (limit, offset, total, next URLs...)
        :rtype: dict
        """

        url = API_ROOT_URL + "/search"
        params = {
            "q": q,
            "type": type,
            "market": market,
            "limit": limit,
            "offset": offset,
            "include_external": include_external
        }

        # 2 different implementations based on `recursive` arg
        if not recursive:
            response = self.http_session.get(url, params = params)

            check_response_json(response)
            check_status_code(response)

            return r_json
        else:   # follow next URLs in response
            # NOTE: this implementation is very hard to test because total amount
            # of items returned from Spotify changes through each request.
            # Maybe it is better to find more testable approaches in the future.
            prep_req = requests.Request("GET", url, params = params).prepare()
                # construct first URL from base URL and params

            # Put URLs to waiting queue - done when queue is empty
            res = defaultdict(list)
            q_urls = Queue()
            q_urls.put_nowait(prep_req.url)

            while not q_urls.empty():
                url = q_urls.get_nowait()
                response = self.http_session.get(url)

                check_response_json(response)
                check_status_code(response)

                r_json = response.json()
                for key in r_json.keys():
                    print(r_json[key]["offset"], r_json[key]["total"], r_json[key]["next"])
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

        url = API_ROOT_URL + "/tracks"
        params = {
            "ids": ",".join(track_id) \
                if isinstance(track_id, List) else track_id,
            market: market
        }
        response = self.http_session.get(url, params = params)

        check_response_json(response)
        check_status_code(response)

        return response.json()
    
    # The remaining endpoints are not implemented 
    # due to not needed in this project
from abc import ABC, abstractmethod
import requests
import base64
import logging


"""Authentication strategies for Spotify WebAPI clients"""
class BaseAuthStrategy(ABC):
    @abstractmethod
    def validate_access_token(self, access_token: dict):
        pass

    @abstractmethod
    def get_access_token(self, client_id: str, client_secret: str) -> dict:
        pass


class ClientCredentialsStrategy(BaseAuthStrategy):
    def __str__(self):
        return "Client Credentials strategy"
    
    def validate_access_token(self, access_token: dict):
        if list(access_token.keys()) != \
            ["access_token", "token_type", "expires_in"]:
            logging.error("Response has unexpected fields")
            raise Exception("Invalid response")

    def get_access_token(self, client_id: str, client_secret: str) -> dict:
        # Prepare auth msg in header in base64 (as specified in API docs)
        auth_str = f"{client_id}:{client_secret}"
        auth_bytes = auth_str.encode("utf-8")
        auth_b64 = base64.b64encode(auth_bytes)
        auth_msg = auth_b64.decode("utf-8")

        accounts_url = "https://accounts.spotify.com/api/token"
        response = requests.post(
            url = accounts_url, 
            data = {"grant_type": "client_credentials"},
            headers = {
                "Authorization": f"Basic {auth_msg}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )

        response.raise_for_status()

        r_json = response.json()
        self.validate_access_token(r_json)
        return r_json
    

class AuthCodeStrategy(BaseAuthStrategy):
    # Only for pattern demonstration purposes. More information:
    # https://developer.spotify.com/documentation/web-api/tutorials/code-flow
    def validate_access_token(self, access_token: dict):
        # Only for pattern demonstration purposes
        raise NotImplementedError()

    def get_access_token(self, client_id: str, client_secret: str) -> dict:
        raise NotImplementedError()
    

class AuthCodePKCEStrategy(BaseAuthStrategy):
    # Only for pattern demonstration purposes. More information:
    # https://developer.spotify.com/documentation/web-api/tutorials/code-pkce-flow
    def validate_access_token(self, access_token: dict):
        raise NotImplementedError()
    
    def get_access_token(self, client_id: str, client_secret: str) -> dict:
        raise NotImplementedError()


"""Class used for client-side authentication to Spotify WebAPI"""
class ClientAuthenticator:
    def __init__(self, client_id: str, client_secret: str):
        self.__client_id = client_id
        self.__client_secret = client_secret

    def set_strategy(self, strategy: BaseAuthStrategy):
        self.strategy = strategy

    def get_access_token(self):
        if not hasattr(self, "strategy"):
            error_msg = "An auth strategy must be set first"
            raise AttributeError(error_msg)
        
        logging.info(f"Getting access token using {self.strategy}")
        access_token = self.strategy.get_access_token(
            self.__client_id, 
            self.__client_secret
        )
        logging.info("Finished getting access token")

        return access_token
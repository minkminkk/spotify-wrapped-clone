from faker.providers import BaseProvider


class SpotifyProvider(BaseProvider):
    def _entity_id(self) -> str:
        return super().hexify("^" * 22)
    
    def user_id(self) -> str:
        """Generate a random user ID (as 22-char hexadecimal string)

        :return: user ID
        :rtype: str
        """

        return self._entity_id()
    
    def track_id(self) -> str:
        """Generate a random track ID (as 22-char hexadecimal string)

        :return: track ID
        :rtype: str
        """

        return self._entity_id()
    
Provider = SpotifyProvider

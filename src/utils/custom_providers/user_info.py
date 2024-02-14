from faker.providers import profile
from . import spotify

providers = (profile.Provider, spotify.Provider)


class UserInfoProvider(*providers):
    def user_profile(self) -> dict:
        """Get user profile information.

        :return: User profile information.
        :rtype: dict
        """

        return {"user_id": super().user_id()} | super().simple_profile()
        

Provider = UserInfoProvider
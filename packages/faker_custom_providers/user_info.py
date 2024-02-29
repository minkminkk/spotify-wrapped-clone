# 
# Provider for generating fake user profile data
# 

from datetime import datetime
from faker.providers import profile
from . import spotify

providers = (profile.Provider, spotify.Provider)


class UserInfoProvider(*providers):
    def user_profile(self) -> dict:
        """Generate user profile information.

        :return: User profile information.
        :rtype: dict
        """

        user_id = super().user_id()
        profile = super().simple_profile()
        profile["birthdate"] = profile["birthdate"].strftime("%Y-%m-%d") 
        return {"user_id": user_id} | profile
        

Provider = UserInfoProvider
from faker.providers import python, profile, misc

providers = (python.Provider, profile.Provider, misc.Provider)


class UserInfoProvider(*providers):
    def user_profile(self) -> dict:
        """Get user profile information.

        :return: User profile information.
        :rtype: dict
        """

        return {"user_id": super().hexify("^" * 22)} \
            | super().simple_profile()
        

Provider = UserInfoProvider
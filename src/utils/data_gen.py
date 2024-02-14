from datetime import datetime

from faker import Faker
from custom_providers import user_info, user_clickstream


def main():
    fake = Faker()
    fake.add_provider(user_info.Provider)
    fake.add_provider(user_clickstream.Provider)

    users = [fake.user_profile() for _ in range(50)]
    for u in users:
        print(u)


    for _ in fake.events_from_users_between(
        start_dt = "2018-01-01 01:00:00",
        end_dt = "2018-01-01 05:00:00",
        user_id_list = list(map(lambda x: x["user_id"], users))
    ):
        print(_)




# def generate_users(no_users: int) -> Generator:
#     """Generate mock user profiles.

#     :param no_users: Number of users to be generated.
#     :type no_users: int 
#     :yield: User info generator.
#     :rtype: Generator
#     """
#     fake = Faker()
#     fake.add_provider(python)
#     fake.add_provider(profile)

#     for _ in range(no_users):
#         yield {
#             "user_id": fake.unique.pystr(
#                 min_chars = SPOTIFY_ID_LEN,
#                 max_chars = SPOTIFY_ID_LEN
#             )
#         } | fake.simple_profile()


# def generate_events(
#     no_events: int,
#     start_ts: datetime, 
#     end_ts: datetime
# ) -> Generator:
    
#     fake = Faker


if __name__ == "__main__":
    main()
from faker import Faker
from custom_providers import user_info, user_clickstream


def main():
    fake = Faker()
    fake.add_provider(user_info.Provider)
    fake.add_provider(user_clickstream.Provider)

    users_cnt = 2
    users = [fake.user_profile() for _ in range(users_cnt)]
    for u in users:
        print(u)


    for _ in fake.events_from_users_between(
        start_dt = "2018-01-01 01:00:00",
        end_dt = "2018-01-01 05:00:00",
        user_id_list = list(map(lambda x: x["user_id"], users))
    ):
        print(_)


if __name__ == "__main__":
    main()
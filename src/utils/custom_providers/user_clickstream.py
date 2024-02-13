from typing import Literal
from datetime import datetime, timedelta
from faker.providers import internet, date_time, user_agent
from random import randint, choice

providers = (
    internet.Provider, 
    date_time.Provider, 
    user_agent.Provider
)

SPOTIFY_ENTITY_ID_FORMAT = "^" * 22


class ClickstreamProvider(*providers):
    def _single_event(self, **kwargs):
        try:
            if len(kwargs["user_id"]) != 22:
                raise ValueError("user_id must consist of 22 hexadecimal characters")
            if kwargs["event_name"] not in ("play", "stop"):
                raise ValueError("event_name must be 'play' or 'stop'")
            
            return {
                "ipv4": kwargs["ipv4"],
                "user_id": kwargs["user_id"],   # 62-char alphanum string
                "user_agent": kwargs["user_agent"],
                "event_ts": kwargs["event_ts"],
                "event_name": kwargs["event_name"],
                "track_id": kwargs["track_id"] 
            }
        except KeyError:
            raise ValueError("Invalid arguments.")
        except Exception as e:
            raise


    def events_per_user(
        self,
        max_events: int = 50,
        start_dt: datetime = datetime.now() - timedelta(days = 1),
        end_dt: datetime = datetime.now()
    ):
        """Generate events for a random user.

        :param max_events: maximum number of events, defaults to 50
        :type max_events: int, optional
        :param start_dt: defaults to datetime.now()-timedelta(hours = 1)
        :type start_dt: datetime, optional
        :param end_dt: defaults to datetime.now()
        :type end_dt: datetime, optional
        :yield: events of a user of random ID
        :rtype: dict
        """
        # Initial parameters
        ipv4 = super().ipv4()
        user_id = super().hexify(SPOTIFY_ENTITY_ID_FORMAT)
        event_name = "play"

        # Implement in list comprehension would throw error
        track_choices = []
        for _ in range(randint(5, 30)):
            track_choices.append(super().hexify(SPOTIFY_ENTITY_ID_FORMAT))
        # 1 user is predetermined to only listen to 5-30 songs
        track_id = choice(track_choices)

        # Initial timestamps and result count
        cur_start_dt = start_dt
        cur_end_dt = min(end_dt, cur_start_dt + timedelta(minutes = 10))
        cur_events = 1

        # Increment timestamps for each record until 
        # end of time period or max events reached
        # After above conditions are met, loop exits at stop event.
        while (cur_start_dt <= cur_end_dt \
            and cur_events <= max_events) \
            or event_name == "stop":   # Avoid user playing a song and never stop
            event_ts = super().date_time_between(
                start_date = cur_start_dt,
                end_date = cur_end_dt
            )

            yield self._single_event(
                ipv4 = ipv4,
                user_id = user_id,
                user_agent = super().user_agent(),
                event_ts = event_ts,
                event_name = event_name,
                track_id = track_id
            )

            cur_events += 1
            if event_name == "stop":
                track_id = choice(track_choices)  # pick next song
                event_name = "play"
                cur_start_dt = event_ts + timedelta(minutes = randint(10, 150))
            else:
                event_name = "stop"
                cur_start_dt = min(
                    end_dt, event_ts + timedelta(seconds = randint(10, 150))
                )       # only exit loop after stop event
            cur_end_dt = min(end_dt, cur_start_dt + timedelta(minutes = 10))


Provider = ClickstreamProvider
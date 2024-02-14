# 
# Provider for generating fake user clickstream data
# 


from typing import Generator, List
import string
from datetime import datetime, timedelta
from random import randint, choice, choices

from faker.providers import internet, date_time, user_agent
from . import spotify

providers = (
    internet.Provider, 
    date_time.Provider, 
    user_agent.Provider,
    spotify.Provider
)


class ClickstreamProvider(*providers):
    def _single_event(self, **kwargs):
        try:
            if len(kwargs["user_id"]) != 22 \
                and all([c in string.hexdigits for c in kwargs["user_id"]]):
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
        except Exception:
            raise

    def tracklist_for_user(
        self, 
        min_tracks: int, 
        max_tracks: int, 
        sample_track_ids: List[str] | None = None
    ) -> List[str]:
        """Generate random tracklist for user.

        :param min_tracks: minimum number of tracks in tracklist
        :type min_tracks: int
        :param max_tracks: maximum number of tracks in tracklist
        :type max_tracks: int
        :param sample_track_ids: sample data that generated data will be based on
        :type sample_track_ids: List[str] | None, optional
        :return: list of track_id
        :rtype: List[str]
        """

        if min_tracks > max_tracks:
            raise ValueError("min_tracks must be smaller or equal to max_tracks")

        num_tracks = randint(min_tracks, max_tracks)
        if sample_track_ids:    # if have sample data then take from sample
            track_choices = choices(sample_track_ids, k = num_tracks)
        else:   # else generate it
            track_choices = []
            for _ in range(num_tracks):
                track_choices.append(super().track_id())
        
        return track_choices

    def events_from_user_between(
        self,
        start_dt: datetime | str,
        end_dt: datetime | str,
        user_id: str,
        max_events: int = 1000
    ) -> Generator[dict, None, None]:
        """Generate events for a particular user between 2 timestamps.
        Used as generators instead of functions like in other providers.

        :param start_dt: start datetime
        :type start_dt: datetime | str
        :param end_dt: end datetime
        :type end_dt: datetime | str
        :param user_id: ID of user
        :type user_id: str
        :param max_events: maximum number of events generated in case the
            time period is too large, defaults to 100
        :type max_events: int, optional
        :raises ValueError: when cannot parse start_dt or end_dt datetime strings
        :yield: information about events
        :rtype: Generator[dict, None, None]
        """
        # Check start_dt, end_dt
        def _parse_dt_str(dt):
            """Parse datetime string into datetime object if is string"""
            if isinstance(dt, str):
                try:
                    dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    raise ValueError("Invalid datetime format.")
            return dt
        start_dt, end_dt = map(_parse_dt_str, (start_dt, end_dt))

        if start_dt > end_dt:
            raise ValueError("start_dt must be <= end_dt.")

        # Initial parameters
        ipv4 = super().ipv4()   # per-user param
        event_name = "play"
        max_events -= 1 if max_events % 2 != 0 else 0   
            # update max_events to be even so that event_name ends after "stop" 

        # Per-user param because we do not want track_id to be completely random
        # Implement in list comprehension would throw error
        tracklist = self.tracklist_for_user(min_tracks = 5, max_tracks = 30)
        track_id = choice(tracklist)

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
                track_id = choice(tracklist)  # pick next song
                event_name = "play"
                cur_start_dt = event_ts + timedelta(minutes = randint(10, 150))
            else:
                event_name = "stop"
                cur_start_dt = min(
                    end_dt, event_ts + timedelta(seconds = randint(10, 150))
                )       # only exit loop after stop event
            cur_end_dt = min(end_dt, cur_start_dt + timedelta(minutes = 10))

    def events_from_users_between(
        self, 
        start_dt: datetime, 
        end_dt: datetime,
        user_id_list: List[str],
        max_events_per_user: int = 1000
    ) -> Generator[dict, None, None]:
        """Generate events for multiple users between 2 timestamps. 
        Used as generators instead of functions like in other providers.

        :param start_dt: start datetime
        :type start_dt: datetime
        :param end_dt: end datetime
        :type end_dt: datetime
        :param user_id_list: list of user_id
        :type user_id_list: List[str]
        :param max_events_per_user: maximum number of events per user, 
            defaults to 1000
        :type max_events_per_user: int, optional
        :yield: information about events
        :rtype: Generator[dict, None, None]
        """

        for user_id in user_id_list:
            for event in self.events_from_user_between(
                start_dt = start_dt,
                end_dt = end_dt,
                user_id = user_id,
                max_events = max_events_per_user
            ):
                yield event


Provider = ClickstreamProvider
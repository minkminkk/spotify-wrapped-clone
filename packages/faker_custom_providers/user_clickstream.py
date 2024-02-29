# 
# Provider for generating fake user clickstream data
# 


from typing import Generator, List
import string
from datetime import datetime, timedelta
from random import randint, choice, sample

from faker.providers import internet, date_time, user_agent, misc
from . import spotify

providers = (
    internet.Provider, 
    date_time.Provider, 
    user_agent.Provider,
    misc.Provider,
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
            
            keys = (
                *("event_id", "event_ts", "event_name"), 
                *("ipv4", "user_id", "user_agent", "track_id")
            )
            return {k:kwargs[k] for k in keys}
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
        :param max_tracks: maximum number of tracks in tracklist
        :param sample_track_ids: sample data that generated data will be based on
        :return: list of track_id
        """

        if min_tracks > max_tracks:
            raise ValueError("min_tracks must be smaller or equal to max_tracks")

        if sample_track_ids:    # if have sample data then take from sample
            # Avoid taking sample larger than population
            max_tracks = min(max_tracks, len(sample_track_ids))
            num_tracks = randint(min_tracks, max_tracks)
            track_sample = sample(sample_track_ids, k = num_tracks)
        else:   # else generate it
            num_tracks = randint(min_tracks, max_tracks)
            track_sample = []
            for _ in range(num_tracks):
                track_sample.append(super().track_id())
        
        return track_sample

    def events_from_one_user(
        self,
        start_dt: datetime | str,
        end_dt: datetime | str,
        user_id: str | None = None,
        user_tracklist: List[str] | None = None,
        max_events: int | None = None
    ) -> Generator[dict, None, None]:
        """Generate events for a particular user between 2 timestamps.
        Used as generators instead of functions like in other providers.

        :param start_dt: start datetime
        :param end_dt: end datetime
        :param user_id: ID of user, randomly generated if not specified
        :param user_tracklist: track IDs from tracks,
            randomly generated if not specified
        :param max_events: maximum number of events generated in case the
            time period is too large, defaults to None
        :yield: information about events
        """
        # user_id, ipv4, user_agent: per user - generated only once per call
        # event_id, event_ts: per event
        # event_name, track_id: per event pair / track listened

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

        # Initial parameters - per-user params
        user_id = super().user_id() if not user_id else user_id
        ipv4 = super().ipv4()
        user_agent = super().user_agent()
        event_name = "play"
        if max_events:
            max_events -= 1 if max_events % 2 != 0 else 0
            # update max_events to be even so that 
            # event generation ends at event_name == "stop" 

        # Per-user param because we do not want track_id to be completely random
        # Implement in list comprehension would throw error
        if not user_tracklist:   # generate if not specified
            user_tracklist = self.tracklist_for_user(
                min_tracks = 5, 
                max_tracks = 30
            )
        else:
            user_tracklist = sample(
                user_tracklist, 
                k = min(randint(5, 30), len(user_tracklist))
            )
        track_id = choice(user_tracklist)

        # Initial timestamps and result count
        cur_start_dt = start_dt
        cur_end_dt = min(end_dt, cur_start_dt + timedelta(minutes = 10))
        cur_events = 1
        max_events = float("inf") if not max_events else max_events

        # Increment timestamps for each record until 
        # end of time period or max events reached
        # After above conditions are met, loop exits at stop event.
        while cur_start_dt <= cur_end_dt and cur_events <= max_events:
            event_ts = super().date_time_between(
                start_date = cur_start_dt,
                end_date = cur_end_dt
            )

            yield self._single_event(
                event_id = super().sha256(),
                event_ts = event_ts,
                event_name = event_name,
                ipv4 = ipv4,
                user_id = user_id,
                user_agent = user_agent,
                track_id = track_id
            )

            cur_events += 1
            if event_name == "stop":
                track_id = choice(user_tracklist)  # pick next song
                event_name = "play"
                cur_start_dt = event_ts + timedelta(minutes = randint(1, 10))
                    # assume new song start 1-10 mins after stop
            else:
                event_name = "stop"
                cur_start_dt = min(
                    end_dt, event_ts + timedelta(seconds = randint(10, 150))
                )       # only exit loop after stop event
            cur_end_dt = min(end_dt, cur_start_dt + timedelta(minutes = 10))

    def events_from_multiple_users(
        self, 
        start_dt: datetime, 
        end_dt: datetime,
        user_id_list: List[str],
        max_events_per_user: int | None = None
    ) -> Generator[dict, None, None]:
        """Generate events for specified list of users between 2 timestamps. 
        Used as generators instead of functions like in other providers.
        Basically events_from_one_user() iterated over list of user_ids.

        :param start_dt: start datetime
        :param end_dt: end datetime
        :param user_id_list: list of user_id
        :param max_events_per_user: maximum number of events per user, 
            defaults to None
        :yield: information about events
        """

        for user_id in user_id_list:
            for event in self.events_from_one_user(
                start_dt = start_dt,
                end_dt = end_dt,
                user_id = user_id,
                max_events = max_events_per_user
            ):
                yield event


Provider = ClickstreamProvider
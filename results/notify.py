from contextlib import contextmanager

from .psyco import quoted_identifier


def start_listening(connection, channels):
    names = [quoted_identifier(each) for each in channels]

    c = connection.cursor()

    for name in names:
        c.execute(f"listen {name};")


class ListenNotify:
    def notifications(self, connection, timeout=5, yield_on_timeout=False):
        """Subscribe to PostgreSQL notifications, and handle them
        in infinite-loop style.

        On an actual message, returns the notification (with .pid,
        .channel, and .payload attributes).

        If you've enabled 'yield_on_timeout', yields None on timeout.
        """

        while True:
            gen = connection.notifies(timeout=timeout)
            try:
                for notify in gen:
                    yield notify

                if yield_on_timeout:
                    yield None
                else:
                    break

            finally:
                gen.close()

    @contextmanager
    def listening_connection(self, channels):
        with self.c_autocommit() as cc:
            if isinstance(channels, str):
                channels = [channels]
            start_listening(cc, channels)
            yield cc

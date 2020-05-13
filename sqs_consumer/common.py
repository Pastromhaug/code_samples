import time
from signal import SIGINT, SIGTERM, signal

from datadog import statsd


def process_message(sqs_message: str) -> None:
    print(f"processing message: {sqs_message}")
    # process your sqs message here
    pass


class SignalHandler:
    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


def wait(seconds: int):
    def decorator(fun):
        last_run = time.monotonic()

        def new_fun(*args, **kwargs):
            nonlocal last_run
            now = time.monotonic()
            if time.monotonic() - last_run > seconds:
                last_run = now
                return fun(*args, **kwargs)

        return new_fun

    return decorator


@wait(seconds=15)
def send_queue_metrics(sqs_queue) -> None:
    print("sending queue metrics")
    statsd.gauge(
        "sqs.queue.message_count",
        queue_length(sqs_queue),
        tags=[f"queue:{queue_name(sqs_queue)}"],
    )


def queue_length(sqs_queue) -> int:
    sqs_queue.load()
    return int(sqs_queue.attributes["ApproximateNumberOfMessages"])


def queue_name(sqs_queue) -> str:
    sqs_queue.load()
    return sqs_queue.attributes["QueueArn"].split(":")[-1]

import os

import boto3

from common import SignalHandler, process_message, send_queue_metrics

sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])
dlq = sqs.get_queue_by_name(QueueName=os.environ["SQS_DEAD_LETTER_QUEUE_NAME"])

if __name__ == "__main__":
    signal_handler = SignalHandler()
    while not signal_handler.received_signal:
        send_queue_metrics(queue)
        send_queue_metrics(dlq)
        messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1,)
        for message in messages:
            try:
                process_message(message.body)
            except Exception as e:
                print(f"exception while processing message: {repr(e)}")
                continue

            message.delete()

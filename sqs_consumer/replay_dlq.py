import os

import boto3

from common import SignalHandler, process_message, queue_length

sqs = boto3.resource("sqs")
dlq = sqs.get_queue_by_name(QueueName=os.environ["SQS_DEAD_LETTER_QUEUE_NAME"])

if __name__ == "__main__":
    signal_handler = SignalHandler()
    while queue_length(dlq) and not signal_handler.received_signal:
        print(f"queue length: {queue_length(dlq)}")
        messages = dlq.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1,)
        for message in messages:
            process_message(message.body)
            message.delete()

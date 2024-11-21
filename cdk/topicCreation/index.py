import boto3
import logging
import os
import json
import time
import socket
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def on_event(event, context):
    logger.info(event)
    request_type = event['RequestType']
    match request_type:
        case 'Create':
            return create_topic(event)
        case 'Update':
            logger.info('Update')
            return create_topic(event)
        case 'Delete':
            logger.info('Delete')
        case _:
            logger.error(f'Unexpected RequestType: {event["RequestType"]}')

    return

def create_topic(event):
    logger.info('Creating Topic')

    bootstrap_servers = event['ResourceProperties']['bootstrap_servers']
    topic_name = event['ResourceProperties']['topic_name']
    num_partitions = int(event['ResourceProperties'].get('num_partitions', 1))
    replication_factor = int(event['ResourceProperties'].get('replication_factor', 1))

    try:
        # Directly connect to Kafka with no token provider
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol='PLAINTEXT',  # Adjust security protocol as needed
            client_id=socket.gethostname(),
        )

        # Define the new topic with the specified properties
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor, topic_configs={'retention.ms': str(24 * 60 * 60 * 1000)})]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()

        logger.info(f"Topic '{topic_name}' created successfully.")
        return {
            'Status': 'SUCCESS',
            'Data': {
                'Message': f"Topic '{topic_name}' created successfully."
            }
        }
    except Exception as e:
        logger.error(f"Failed to create topic: {str(e)}")
        return {
            'Status': 'FAILED',
            'Data': {
                'Message': f"Failed to create topic: {str(e)}"
            }
        }

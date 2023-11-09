import socket
import os
import threading
from header import Header
from collections import defaultdict
#import re
# import pygame

# pygame.init()

# Packet types
PACKET_TYPE_SUBSCRIPTION = 'S'
PACKET_TYPE_PUBLICATION = 'P'
PACKET_TYPE_ACKNOWLEDGMENT = 'A'
PACKET_TYPE_UNSUBSCRIPTION = 'U'

SAVE_PATH = "C:\\Users\\eliel\\Documents\\College\\3rd year\\Computer Networks\\Notes & Assignments\\Assignment " \
            "1\\SaveImages "

# Consumer configuration
CONSUMER_IP = "consumer2"
CONSUMER_PORT = 60001
BUFFER_SIZE = 65507
BROKER_IP = "broker"
BROKER_PORT = 50000
broker_address = (BROKER_IP, BROKER_PORT)

consumer2_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
consumer2_socket.bind((CONSUMER_IP, CONSUMER_PORT))

ongoing_transmissions = defaultdict(list)


def message_handler():
    global ongoing_transmissions

    while True:
        data = consumer2_socket.recvfrom(BUFFER_SIZE)
        print(f"received data from broker")
        header = Header.from_bytes(data[0][:16])

        if header.packet_type == PACKET_TYPE_PUBLICATION:
            ongoing_transmissions[header.producer_id + header.stream_id].append(data[0][16:16 + header.frame_length])

            print(f"Received data: {data}")

            if len(ongoing_transmissions[header.producer_id + header.stream_id]) == int(header.frame_number):
                # All chunks have been received
                payload_data = b''.join(ongoing_transmissions[header.producer_id + header.stream_id])

                if header.stream_id == "01":  # For images
                    filename = f"{header.producer_id}_{header.stream_id}_{header.frame_number}.png"
                    try:
                        with open(os.path.join(
                                SAVE_PATH, filename), 'wb') as img_file:
                            img_file.write(payload_data)
                        print(f"Saved image: {filename}")
                    except Exception as e:
                        print(f"Error saving image: {e}")

                    hex_content = payload_data.hex()
                    print(f"Received: {hex_content[:20]}...")

                elif header.stream_id == "02":  # For audio
                    filename = f"{header.producer_id}_{header.stream_id}_{header.frame_number}.m4v"
                    with open(filename, 'wb') as audio_file:
                        audio_file.write(payload_data)
                    print(f"Saved audio: {filename}")

                elif header.stream_id == "03":  # For text
                    print("Received message: " + payload_data.decode())

                del ongoing_transmissions[header.producer_id + header.stream_id]


# Start the message_handler as a thread
threading.Thread(target=message_handler, daemon=True).start()

while True:
    try:
        action = input("Enter an action (subscribe, unsubscribe, exit): ")

        # The rest of the logic for subscribe, unsubscribe and exit...
        if action.lower() == 'exit':
            break
        elif action.lower() == 'subscribe':
            topic = input("Enter a topic to subscribe (e.g., ABCD01): ")  # Changed the example
            # match = re.search(r'ABCD(\d{2})$', topic)
            # if match:
            #     topic = match.group(1)
            # else:
            #     print("Invalid topic name.")
            #     continue
            topic = topic[-2:].zfill(2)
            print(f"sending topic: {topic}")
            subscription_header = Header(PACKET_TYPE_SUBSCRIPTION, "ABCD99", "00", topic, "00", 0)
            print(f"Sending Header: Packet Type: {subscription_header.packet_type}, Producer ID: {subscription_header.producer_id}, Content Type: {subscription_header.content_type}, Stream ID: {subscription_header.stream_id}, Frame Number: {subscription_header.frame_number}, Frame Length: {subscription_header.frame_length}")
            consumer2_socket.sendto(subscription_header.to_bytes(), broker_address)
            print(f"Subscribed to topic: {topic}")
        elif action.lower() == 'unsubscribe':
            topic = input("Enter a topic to unsubscribe (e.g., ABCD01): ")  # Changed the example
            topic = topic[-2:].zfill(2)  # This line can be removed
            unsubscription_header = Header(PACKET_TYPE_UNSUBSCRIPTION, "000000", "00", topic, "00", 0)
            consumer2_socket.sendto(unsubscription_header.to_bytes(), broker_address)
            print(f"Unsubscribed from topic: {topic}")
    except KeyboardInterrupt:
        print(f"Consumer shutting down...")
        consumer2_socket.close()
        break


# Close the socket
consumer2_socket.close()

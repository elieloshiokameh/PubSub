import socket
from header import Header

# Broker configuration
BROKER_IP = "broker"
BROKER_PORT = 50000
BUFFER_SIZE = 65507
HEADER_SIZE = 18

broker_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
broker_socket.bind((BROKER_IP, BROKER_PORT))

print("Broker listening on {}:{}".format(BROKER_IP, BROKER_PORT))

# Map stream IDs to set's of consumers who are subscribed to them
subscriptions = {}

while True:
    try:
        data, address = broker_socket.recvfrom(BUFFER_SIZE)
        # print(f"Raw data {data}")
        if len(data) <= 15:
            print("Received malformed packet")
            continue

        try:
            header = Header.from_bytes(data)
            print(f"Decoded Header: Packet Type: {header.packet_type}, Producer ID: {header.producer_id}, Content Type: {header.content_type}, Stream ID: {header.stream_id}, Frame Number: {header.frame_number}, Frame Length: {header.frame_length}")

        except UnicodeDecodeError:
            print("Error decoding packet data: UnicodeDecodeError")
            continue
        except Exception as e:
            print(f"Error decoding packet data: {e}")
            continue

        if header.packet_type == "P":
            subscribers = subscriptions.get(header.stream_id, set())
            if subscribers:
                print(
                    f"Received data from Producer {header.producer_id}, Stream {header.stream_id}, Frame {header.frame_number}")

            ack_msg = f"Ack for Frame {header.frame_number} of Stream {header.stream_id} from Producer {header.producer_id} "
            ACK = b"A" + ack_msg.encode()
            broker_socket.sendto(ACK, address)

            for consumer_address in subscribers:
                print(f"sending data to {consumer_address} consumer")
                broker_socket.sendto(data, consumer_address)
                print(f"data sent to {consumer_address} consumer")

        elif header.packet_type == "S":
            if header.stream_id not in subscriptions:
                subscriptions[header.stream_id] = set()
            subscriptions[header.stream_id].add(address)
            print(f"consumer {address} subscribed to topic: {header.stream_id}")

        elif header.packet_type == "U":
            if header.stream_id in subscriptions and address in subscriptions[header.stream_id]:
                subscriptions[header.stream_id].remove(address)
                print(f"consumer {address} unsubscribed from topic: {header.stream_id}")

    except KeyboardInterrupt:
        print("Broker socket shutting down...")
        broker_socket.close()
        break

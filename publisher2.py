import socket
import os
import time
from header import Header

# Publisher configuration
BROKER_IP = "broker"
BROKER_PORT = 50000
BUFFER_SIZE = 65507
CHUNK_SIZE = 60000
MAX_RETRIES = 3  # Number of times we'll try resending a packet
RETRY_TIMEOUT = 2  # Time (in seconds) to wait for acknowledgment before retrying

# Create UDP socket at the publisher side
publisher_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

print("UDP publisher ready")

def send_and_acknowledge(data, content_type, stream_id, filename=None):
    send_chunked_data(data, content_type, stream_id)

    retries = 0
    while retries < MAX_RETRIES:
        publisher_socket.settimeout(RETRY_TIMEOUT)
        try:
            ack, _ = publisher_socket.recvfrom(BUFFER_SIZE)
            print(ack.decode())
            break
        except socket.timeout:
            print(f"No acknowledgment received for {filename if filename else content_type}. Retrying...")
            retries += 1
    if retries == MAX_RETRIES:
        print(f"Failed to receive acknowledgment for {filename if filename else content_type} after {MAX_RETRIES} attempts.")

def send_chunked_data(data, content_type, stream_id):
    total_chunks = len(data) // CHUNK_SIZE
    if len(data) % CHUNK_SIZE:
        total_chunks += 1

    for chunk_num in range(total_chunks):
        start = chunk_num * CHUNK_SIZE
        end = (chunk_num + 1) * CHUNK_SIZE

        chunk_data = data[start:end]

        packet = Header("P", producer_id, content_type, stream_id, str(chunk_num).zfill(4),
                        len(chunk_data)).to_bytes() + chunk_data

        publisher_socket.sendto(packet, (BROKER_IP, BROKER_PORT))
        print(f"Published {content_type} chunk {chunk_num + 1}/{total_chunks}")
        time.sleep(0.5)

while True:
    try:
        action = input("Enter an action (publish, exit): ")

        if action.lower() == 'exit':
            print(f"Publisher shutting down...")
            break
        elif action.lower() == 'publish':
            producer_id = input("Enter your 6 Character Producer ID (e.g., ABC123): ")
            content_type = input(
                f"Choose content type ('01' for image, '02' for text, '03' for audio): ")
            stream_id = input("Enter the stream id (e.g., 01, 02, 03, 04): ")

            if content_type.lower() == "01":
                folder_path = input("Enter the path to the folder containing Video Frames: ")
                for filename in os.listdir(folder_path):
                    if filename.endswith(".png"):
                        with open(os.path.join(folder_path, filename), 'rb') as img_file:
                            img_byte_arr = img_file.read()
                            send_and_acknowledge(img_byte_arr, content_type, stream_id, filename)

            elif content_type.lower() == "02":
                text_content = input("Enter your message: ")
                text_byte_arr = text_content.encode()
                send_and_acknowledge(text_byte_arr, content_type, stream_id)

            elif content_type.lower() == "03":
                file_path = input("Enter the path to the audio file (.m4v): ")
                if not file_path.endswith('.m4v'):
                    print("Invalid audio format. Please provide a .m4v file.")
                    continue
                with open(file_path, 'rb') as audio_file:
                    audio_byte_arr = audio_file.read()
                    send_and_acknowledge(audio_byte_arr, content_type, stream_id)

    except KeyboardInterrupt:
        print(f"Publisher shutting down...")
        publisher_socket.close()
        break

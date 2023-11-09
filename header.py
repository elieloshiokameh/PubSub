class Header:
    def __init__(self, packet_type, producer_id, content_type, stream_id, frame_number, frame_length):
        self.packet_type = packet_type
        self.producer_id = producer_id
        self.content_type = content_type
        self.stream_id = stream_id
        self.frame_number = frame_number
        self.frame_length = frame_length

    def to_bytes(self):
        return (
                self.packet_type.encode('utf-8') +
                self.producer_id.encode('utf-8') +
                self.content_type.encode('utf-8') +
                self.stream_id.rjust(2, '0').encode('utf-8') +
                self.frame_number.encode('utf-8') +
                self.frame_length.to_bytes(4, byteorder='big')
        )

    @classmethod
    def from_bytes(cls, data):
        packet_type = data[0:1].decode('utf-8')
        producer_id = data[1:7].decode('utf-8')
        content_type = data[7:9].decode('utf-8')
        # print(f"Full data content: {data}")
        stream_id_bytes = data[9:11]  # extract bytes first without decoding
        # print(f"Sliced Stream ID Bytes: {stream_id_bytes}")
        stream_id = stream_id_bytes.decode('utf-8')
        # print(f"stream_id_bytes: {stream_id_bytes}")  # debug line
        frame_number = data[12:14].decode('utf-8')
        frame_length = int.from_bytes(data[14:18], byteorder='big')
        return cls(packet_type, producer_id, content_type, stream_id, frame_number, frame_length)

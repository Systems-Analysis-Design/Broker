package broker.service;

import broker.dto.FileRecordDto;
import broker.dto.MessageDto;
import broker.dto.MessageFileRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class MessageWriteService {
    private static int id = 0;
    private static Map<String, Integer> PARTITION_NAME_MESSAGE_OFFSET_MAP = new HashMap<>() {{
        put("file.txt", 0);
    }};
    private static Map<String, List<MessageFileRecord>> PARTITION_NAME_MESSAGE_FILE_RECORD_MAP = new HashMap<>() {{
        put("file.txt", new ArrayList<>());
    }};
    private final FileService fileService = new FileService();
    private final ObjectMapper mapper = new ObjectMapper();

    public void push(String partitionName, MessageDto message) {
        FileRecordDto fileRecordDto = fileService.write(partitionName, serialize(message));
        MessageFileRecord messageFileRecord = new MessageFileRecord(id, fileRecordDto.startingByte(), fileRecordDto.endingByte());
        id++;
        PARTITION_NAME_MESSAGE_FILE_RECORD_MAP.get(partitionName).add(messageFileRecord);
    }

    public MessageDto pull(String partitionName) {
        Integer messageNumber = PARTITION_NAME_MESSAGE_OFFSET_MAP.get(partitionName);
        Optional<MessageFileRecord> first = PARTITION_NAME_MESSAGE_FILE_RECORD_MAP.get(partitionName)
                                                                                  .stream()
                                                                                  .filter(x -> x.messageNumber() == messageNumber)
                                                                                  .findFirst();
        if (first.isEmpty()) {
            return null;
        }
        MessageFileRecord messageFileRecord = first.get();
        int length = (int) (messageFileRecord.endingByte() - messageFileRecord.startingByte());
        PARTITION_NAME_MESSAGE_OFFSET_MAP.put(partitionName, messageNumber + 1);
        return deserialize(fileService.read(partitionName, (int) messageFileRecord.startingByte(), length));
    }

    private String serialize(final MessageDto message) {
        try {
            return mapper.writeValueAsString(message);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MessageDto deserialize(final String message) {
        try {
            return mapper.readValue(message, MessageDto.class);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

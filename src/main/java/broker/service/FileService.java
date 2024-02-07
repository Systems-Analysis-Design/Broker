package broker.service;

import broker.dto.FileRecordDto;

import java.io.*;

public class FileService {
    public FileRecordDto write(String fileName, String data) {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            long startPosition = raf.length();
            raf.seek(startPosition);
            raf.writeBytes(data);
            long endPosition = raf.length();
            return new FileRecordDto(startPosition, endPosition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String read(String fileName, int startingPosition, int length) {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            raf.seek(startingPosition);
            byte[] data = new byte[length];
            raf.read(data, 0, length);
            return new String(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

package rabbitmq.filemessage;

import java.io.Serializable;

public class FileMessage implements Serializable {
    private String filename;
    private byte[] content;

    public FileMessage(String filename, byte[] content) {
        this.filename = filename;
        this.content = content;
    }

    public FileMessage() {
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    @Override
    /**
     * Quando stampo a video, non faccio la conversione da array di
     * byte a "lista di numeri" o viene un output troppo lungo
     */
    public String toString() {
        return "{FileMessage: "+filename+", "+ content +"}";
    }
}

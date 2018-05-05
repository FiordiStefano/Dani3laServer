/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package danisync.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import packet.protoPacket.resp;

/**
 *
 * @author Stefano Fiordi
 */
public class FileHandlerServer {

    /**
     * Dimensione dei pacchetti
     */
    private final int PacketLength = 4096;
    /**
     * File da sincronizzare
     */
    protected File ServerFile;
    /**
     * L'indice di partenza per il trasferimento del file
     */
    protected int startIndex;
    /**
     * Numero di pacchetti
     */
    protected long nPackets;
    /**
     * Canale di lettura del file
     */
    protected FileChannel fcServerRead;
    /**
     * Canale di scrittura del file
     */
    protected FileChannel fcServerWrite;
    /**
     * Dimensione dei pezzi
     */
    protected int ChunkSize;
    /**
     * Numero di pezzi
     */
    protected long nChunks;
    /**
     * Pezzo da ricevere
     */
    protected byte[] ChunkToRecv;
    /**
     * Numero di pacchetti del pezzo da ricevere
     */
    protected long nChunkPackets;
    /**
     * Array di digest crc
     */
    protected long[] digests;
    /**
     * File indice contenente i digest crc
     */
    protected File crcIndex;
    /**
     * Canale di scrittura del file indice
     */
    protected FileChannel fcCRCIndex;
    /**
     * Numero di pacchetti del file indice
     */
    protected long nCRCIndexPackets;
    /**
     * Versione del file determinata dal crc calcolato sul file indice
     */
    protected long version;

    /**
     * Costruttore che crea il FileChannel di lettura e scrittura sul file,
     * calcola il numero di pacchetti e di pezzi in base alle dimensioni del
     * file e, se esiste, legge il file indice contenente i digest e la versione
     * del file
     *
     * @param ServerFile il file da sincronizzare
     * @param FileLength la grandezza del file
     * @param ChunkSize la grandezza dei pezzi
     * @throws IOException se la creazione del canale non va a buon fine
     * @throws NumberFormatException se ci sono errori di lettura del file
     * indice
     * @throws MyExc se c'è un errore nella lettura della versione
     */
    public FileHandlerServer(File ServerFile, long FileLength, int ChunkSize) throws IOException, NumberFormatException, MyExc {
        this.ServerFile = ServerFile;
        this.ChunkSize = ChunkSize;
        if (this.ServerFile.exists()) {
            startIndex = -1;
            this.fcServerRead = new FileInputStream(this.ServerFile).getChannel();
            this.fcServerWrite = FileChannel.open(this.ServerFile.toPath(), StandardOpenOption.WRITE);

            if (this.ServerFile.length() % 2 == 0) {
                nPackets = this.ServerFile.length() / PacketLength;
                nChunks = this.ServerFile.length() / this.ChunkSize;
            } else {
                nPackets = this.ServerFile.length() / PacketLength + 1;
                nChunks = this.ServerFile.length() / this.ChunkSize + 1;
            }
        } else {
            startIndex = 0;
            ServerFile.createNewFile();
            this.fcServerRead = new FileInputStream(this.ServerFile).getChannel();
            this.fcServerWrite = FileChannel.open(this.ServerFile.toPath(), StandardOpenOption.WRITE);
            
            if (FileLength % 2 == 0) {
                nPackets = FileLength / PacketLength;
                nChunks = FileLength / this.ChunkSize;
            } else {
                nPackets = FileLength / PacketLength + 1;
                nChunks = FileLength / this.ChunkSize + 1;
            }
        }

        this.crcIndex = new File("Indexes\\" + this.ServerFile.getName() + ".crc");
        this.fcCRCIndex = FileChannel.open(this.crcIndex.toPath(), StandardOpenOption.WRITE);
        if (this.crcIndex.exists()) {
            readDigests();
        }
    }

    protected resp getInfoRespPacket() {
        resp ProtoInfoRespPacket;
        if (this.startIndex != -1) {
            ProtoInfoRespPacket = resp.newBuilder()
                    .setRes("ok")
                    .setInd(this.startIndex)
                    .build();
        } else {
            ProtoInfoRespPacket = resp.newBuilder()
                    .setRes("fae") // fae = file already exists
                    .build();
        }

        return ProtoInfoRespPacket;
    }

    /**
     * Crea il digest CRC32 di un array binario
     *
     * @param packet array binario
     * @return il digest
     * @throws NoSuchAlgorithmException
     */
    private long CRC32Hashing(byte[] packet) {
        Checksum checksum = new CRC32();
        checksum.update(packet, 0, packet.length);

        return checksum.getValue();
    }

    /**
     * Trasforma un buffer binario in un array binario
     *
     * @param buf buffer binario
     * @return l'array binario
     */
    private byte[] getByteArray(ByteBuffer buf) {
        buf.flip();
        byte[] chunk = new byte[buf.remaining()];
        buf.get(chunk);
        buf.clear();

        return chunk;
    }

    /**
     * Scrive su un file indice tutti i digest
     *
     * @throws IOException se ci sono errori durante la scrittura
     */
    private void writeDigests() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(crcIndex, false));
        String s;
        for (long l : digests) {
            s = Long.toString(l);
            while (s.length() < 10) {
                s = "0" + s;
            }
            writer.write(s);
        }

        writer.close();

        writer = new BufferedWriter(new FileWriter(crcIndex, true));
        version = CRC32Hashing(Files.readAllBytes(crcIndex.toPath()));
        s = Long.toString(version);
        while (s.length() < 10) {
            s = "0" + s;
        }
        writer.write(s);
        writer.close();
    }

    /**
     * Legge i digest da un file indice
     *
     * @throws IOException se ci sono errori durante la lettura
     * @throws NumberFormatException se il digest letto presenta caratteri
     * differenti da numeri
     * @throws MyExc se la versione non viene letta correttamente
     */
    private void readDigests() throws IOException, NumberFormatException, MyExc {
        BufferedReader reader = new BufferedReader(new FileReader(crcIndex));
        char[] s = new char[10];
        digests = new long[(int) nChunks];
        int len;
        for (int i = 0; i < (int) nChunks; i++) {
            if ((len = reader.read(s)) != -1) {
                digests[i] = Long.parseLong(new String(s));
            }
        }

        if ((len = reader.read(s)) != -1) {
            version = Long.parseLong(new String(s));
        } else {
            throw new MyExc("Error while reading file version");
        }
        reader.close();
    }

}

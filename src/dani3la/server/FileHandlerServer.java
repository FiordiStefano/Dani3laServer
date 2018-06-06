/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dani3la.server;

import com.google.protobuf.ByteString;
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
import dani3la.packet.protoPacket.resp;
import dani3la.packet.protoPacket.data;
import dani3la.packet.protoPacket.crcReq;

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
    //protected int startIndex;
    /**
     * Numero di pacchetti
     */
    //protected long nPackets;
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
    protected long oldChunks;
    /**
     * Pezzo da ricevere
     */
    protected byte[] ChunkToRecv;
    /**
     * Numero di pacchetti del pezzo da ricevere
     */
    protected long nChunkPackets;
    /**
     * Array di digest
     */
    protected AIndexedArray aiaOld;
    /**
     * File indice contenente i digest della vecchia versione del file;
     */
    protected File oldCRCIndex;
    /**
     * Versione del file attuale
     */
    protected long oldVersion;
    /**
     * Array di digest della nuova versione
     */
    protected IndexedArray iaNew;
    /**
     * Dimensione del nuovo file
     */
    protected long newLength;
    /**
     * File indice contenente i digest della nuova versione
     */
    protected File newCRCIndex;
    /**
     * Numero di pacchetti del nuovo file indice
     */
    protected long nCRCIndexPackets;
    /**
     * Versione del nuovo file
     */
    protected long newVersion;
    /**
     * Numero di chunk del nuovo file
     */
    protected long newChunks;
    /**
     * Contatore dei retry
     */
    protected int RetryCount;

    /**
     * Costruttore che crea il FileChannel di lettura e scrittura sul file,
     * calcola il numero di pacchetti e di pezzi in base alle dimensioni del
     * file e, se esiste, legge il file indice contenente i digest e la versione
     * del file
     *
     * @param ServerFile il file da sincronizzare
     * @param FileLength la grandezza del file
     * @param newCRCLength dimensione del nuovo file indice
     * @param ChunkSize la grandezza dei pezzi
     * @throws IOException se la creazione del canale non va a buon fine
     * @throws NumberFormatException se ci sono errori di lettura del file
     * indice
     * @throws MyExc se c'è un errore nella lettura della versione
     */
    public FileHandlerServer(File ServerFile, long FileLength, long newCRCLength, int ChunkSize) throws IOException, NumberFormatException, MyExc {
        this.ServerFile = ServerFile;
        this.ChunkSize = ChunkSize;
        this.newLength = FileLength;

        if (!this.ServerFile.exists()) {
            ServerFile.createNewFile();
        }
        this.fcServerRead = new FileInputStream(this.ServerFile).getChannel();
        this.fcServerWrite = FileChannel.open(this.ServerFile.toPath(), StandardOpenOption.WRITE);

        if (this.ServerFile.length() % this.ChunkSize == 0) {
            oldChunks = this.ServerFile.length() / this.ChunkSize;
        } else {
            oldChunks = this.ServerFile.length() / this.ChunkSize + 1;
        }
        if (!new File("Indexes\\").exists()) {
            new File("Indexes\\").mkdir();
        }
        this.oldCRCIndex = new File("Indexes\\" + this.ServerFile.getName() + ".crc");
        if (this.oldCRCIndex.exists()) {
            readDigests();
        } else {
            aiaOld = new AIndexedArray(new long[0]);
        }
        if (!new File("Temp\\").exists()) {
            new File("Temp\\").mkdir();
        }
        this.newCRCIndex = new File("Temp\\" + this.ServerFile.getName() + ".crc");
        if (newCRCLength % this.PacketLength == 0) {
            nCRCIndexPackets = newCRCLength / this.PacketLength;
        } else {
            nCRCIndexPackets = newCRCLength / this.PacketLength + 1;
        }
        this.newCRCIndex.createNewFile();
        if (FileLength % this.ChunkSize == 0) {
            newChunks = FileLength / this.ChunkSize;
        } else {
            newChunks = FileLength / this.ChunkSize + 1;
        }
    }

    /**
     * Metodo che compara gli array dei digests e riempie gli array di indici
     * secondari
     *
     * @throws MyExc in caso di errore di riallocazione di un'array
     */
    protected void compareIndexes() throws MyExc {
        for (int i = 0; i < oldChunks; i++) {
            for (int j = 0; j < newChunks; j++) {
                if (iaNew.sIndexes[j] == 0) {
                    if (aiaOld.array[i] == iaNew.array[j]) {
                        aiaOld.addIndex(i, j);
                        iaNew.sIndexes[j]++;
                    }
                } else if (i == j) {
                    if (aiaOld.array[i] == iaNew.array[j]) {
                        aiaOld.addIndex(i, j);
                    }
                }
            }
            if (aiaOld.sIndexes[i].length == 0) {
                aiaOld.addIndex(i, -1);
            }
        }
    }

    protected int countChunksToDownload() {
        int count = 0;
        for (long newInd : iaNew.array) {
            int i = 0;
            for (; i < aiaOld.array.length; i++) {
                if (newInd == aiaOld.array[i]) {
                    break;
                }
            }
            if (i == aiaOld.array.length) {
                count++;
            }
        }

        return count;
    }

    /**
     * Inserimento del pezzo di file ricevuto alla posizione data
     *
     * @param index la posizione in cui inserire il pezzo
     * @throws IOException se ci sono errori nell'accesso al file
     */
    protected void insertChunk(int index) throws IOException {
        fcServerWrite.write(ByteBuffer.wrap(ChunkToRecv), (long) index * ChunkSize);
    }

    /**
     * Metodo che inizializza il pezzo di file della grandezza data
     *
     * @param ChunkLength la grandezza del pezzo di file
     */
    protected void setChunkToRecv(int ChunkLength) {
        ChunkToRecv = new byte[ChunkLength];
        if (ChunkLength % this.PacketLength == 0) {
            nChunkPackets = ChunkLength / PacketLength;
        } else {
            nChunkPackets = ChunkLength / PacketLength + 1;
        }
    }

    protected resp getChunkInfoRespPacket() {
        resp chunkInfoRespPacket;
        chunkInfoRespPacket = resp.newBuilder()
                .setRes("ok")
                .setInd(0)
                .build();

        return chunkInfoRespPacket;
    }

    /**
     * Metodo che aggiunge un pacchetto dati al pezzo di file da ricevere
     *
     * @param packet il pacchetto dati
     * @param packetIndex l'indice del pacchetto
     * @return il pacchetto di risposta
     */
    protected resp addChunkPacket(data packet, int packetIndex) {
        resp respPacket;

        if (packet.getNum() == packetIndex) {
            byte[] bPacket = packet.getDat().toByteArray();
            for (int i = 0; i < bPacket.length; i++) {
                ChunkToRecv[packetIndex * PacketLength + i] = bPacket[i];
            }

            respPacket = resp.newBuilder()
                    .setRes("ok")
                    .build();

            RetryCount = 0;
        } else {
            if (RetryCount < 3) {
                respPacket = resp.newBuilder()
                        .setRes("wp") // wp: wrong packet
                        .setInd(packetIndex) // right packet index
                        .build();
                RetryCount++;
            } else {
                respPacket = resp.newBuilder()
                        .setRes("mrr") // mrr: max retry reached
                        .build();
                RetryCount = 0;
            }
        }

        return respPacket;
    }

    protected crcReq getNewCRCRequest(long newVersion) {
        return crcReq.newBuilder()
                .setCrc(newCRCIndex.getName())
                .setVer(newVersion)
                .build();
    }

    protected resp getCRCIndexInfoRespPacket() {
        resp CRCIndexInfoRespPacket;
        CRCIndexInfoRespPacket = resp.newBuilder()
                .setRes("ok")
                .setInd(0)
                .build();

        return CRCIndexInfoRespPacket;
    }

    /**
     * Metodo che aggiunge il pacchetto ricevuto al file indice in caso il
     * numero sia corretto, altrimenti richiede il pacchetto con il numero
     * corretto, per un massimo di tre volte
     *
     * @param packet Il pacchetto da aggiungere al file
     * @param packetIndex l'indice corretto
     * @return il pacchetto di risposta
     * @throws IOException se ci sono errori durante la scrittura del pacchetto
     */
    protected resp addCRCIndexPacket(data packet, int packetIndex) throws IOException {
        resp respPacket;

        if (packet.getNum() == packetIndex) {
            ByteString bsPacket = packet.getDat();
            // accodo il pacchetto al file
            Files.write(newCRCIndex.toPath(), bsPacket.toByteArray(), StandardOpenOption.APPEND);

            respPacket = resp.newBuilder()
                    .setRes("ok")
                    .build();

            RetryCount = 0;
        } else {
            if (RetryCount < 3) {
                respPacket = resp.newBuilder()
                        .setRes("wp") // wp: wrong packet
                        .setInd(packetIndex) // right packet index
                        .build();
                RetryCount++;
            } else {
                respPacket = resp.newBuilder()
                        .setRes("mrr") // mrr: max retry reached
                        .build();
                RetryCount = 0;
            }
        }

        return respPacket;
    }

    /**
     * Metodo che crea il pacchetto di risposta al pacchetto informazioni
     *
     * @return il pacchetto di risposta
     */
    /*protected resp getInfoRespPacket() {
        resp infoRespPacket;
        if (this.startIndex != -1) {
            infoRespPacket = resp.newBuilder()
                    .setRes("ok")
                    .setInd(this.startIndex)
                    .build();
        } else {
            infoRespPacket = resp.newBuilder()
                    .setRes("fae")
                    .build();
        }

        return infoRespPacket;
    }*/
    /**
     * Metodo che aggiunge il pacchetto ricevuto al file in caso il numero sia
     * corretto, altrimenti richiede il pacchetto con il numero corretto, per un
     * massimo di tre volte
     *
     * @param packet Il pacchetto da aggiungere al file
     * @param packetIndex l'indice corretto
     * @return il pacchetto protobuf di risposta
     * @throws IOException
     */
    /*public resp addPacket(data packet, int packetIndex) throws IOException {
        resp respPacket;

        if (packet.getNum() == packetIndex) {
            ByteString bsPacket = packet.getDat();
            // accodo il pacchetto al file
            Files.write(ServerFile.toPath(), bsPacket.toByteArray(), StandardOpenOption.APPEND);

            respPacket = resp.newBuilder()
                    .setRes("ok")
                    .build();

            RetryCount = 0;
        } else {
            if (RetryCount < 3) {
                respPacket = resp.newBuilder()
                        .setRes("wp") // wp: wrong packet
                        .setInd(packetIndex) // right packet index
                        .build();
                RetryCount++;
            } else {
                respPacket = resp.newBuilder()
                        .setRes("mrr") // mrr: max retry reached
                        .build();
                RetryCount = 0;
            }
        }

        return respPacket;
    }*/
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
    protected byte[] getByteArray(ByteBuffer buf) {
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
    protected void writeDigests() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(oldCRCIndex, false));
        String s;
        for (long l : aiaOld.array) {
            s = Long.toString(l);
            while (s.length() < 10) {
                s = "0" + s;
            }
            writer.write(s);
        }

        writer.close();

        if (aiaOld.array.length > 1) {
            writer = new BufferedWriter(new FileWriter(oldCRCIndex, true));
            oldVersion = CRC32Hashing(Files.readAllBytes(oldCRCIndex.toPath()));
            s = Long.toString(oldVersion);
            while (s.length() < 10) {
                s = "0" + s;
            }
            writer.write(s);
            writer.close();
        } else {
            oldVersion = aiaOld.array[0];
        }
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
        BufferedReader reader = new BufferedReader(new FileReader(oldCRCIndex));
        char[] s = new char[10];
        long[] digests = new long[(int) oldChunks];
        int len;
        for (int i = 0; i < (int) oldChunks; i++) {
            if ((len = reader.read(s)) != -1) {
                digests[i] = Long.parseLong(new String(s));
            }
        }
        aiaOld = new AIndexedArray(digests);

        if (aiaOld.array.length > 1) {
            if ((len = reader.read(s)) != -1) {
                oldVersion = Long.parseLong(new String(s));
            } else {
                throw new MyExc("Error while reading file version");
            }
        } else {
            oldVersion = aiaOld.array[0];
        }
        reader.close();
    }

    /**
     * Legge i digest dal nuovo file indice
     *
     * @throws IOException se ci sono errori durante la lettura
     * @throws NumberFormatException se il digest letto presenta caratteri
     * differenti da numeri
     * @throws MyExc se la versione non viene letta correttamente
     */
    protected void readNewDigests() throws IOException, NumberFormatException, MyExc {
        BufferedReader reader = new BufferedReader(new FileReader(newCRCIndex));
        char[] s = new char[10];
        long[] digests = new long[(int) newChunks];
        int len;
        for (int i = 0; i < (int) newChunks; i++) {
            if ((len = reader.read(s)) != -1) {
                digests[i] = Long.parseLong(new String(s));
            }
        }
        iaNew = new IndexedArray(digests);

        if ((len = reader.read(s)) != -1) {
            newVersion = Long.parseLong(new String(s));
        } else {
            throw new MyExc("Error while reading file version");
        }
        reader.close();
    }

}

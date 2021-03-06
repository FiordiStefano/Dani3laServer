/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dani3la.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import packet.protoPacket.crcInfo;
import packet.protoPacket.crcReq;
import packet.protoPacket.data;
import packet.protoPacket.info;
import packet.protoPacket.resp;
import packet.protoPacket.chunkReq;

/**
 * Classe principale che lancia il server e gestisce la sincronizzazione
 *
 * @author Stefano Fiordi
 */
public class Dani3laServer {

    static ServerSocket ss;
    static Socket socket;
    static File syncDir;
    static int ChunkSize;
    static File Files[];
    static FileHandlerServer SyncFiles[];
    static int RetryCount = 0;
    static boolean delete = true;
    static Logger logger;

    /**
     * Scrive la configurazione del server sul file sConfig.ini
     *
     * @throws IOException se ci sono errori durante la scrittura
     */
    public static void writeDefaultConfig() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("sConfig.ini"), false));
        String s = "Path = SyncDir\\";
        writer.write(s);
        writer.newLine();
        writer.write("Delete = 1");

        writer.close();
    }

    /**
     * Legge la configurazione del server dal file sConfig.ini
     *
     * @throws IOException se ci sono errori durante la lettura
     */
    public static void readConfig() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File("sConfig.ini")));
        String s;
        s = reader.readLine();
        if (s != null && s.contains(" = ")) {
            syncDir = new File(s.split(" = ")[1]);
            s = reader.readLine();
            if (s != null && s.contains(" = ") && s.split(" = ")[1].equals("0")) {
                delete = false;
            }
            if (!syncDir.exists() || !syncDir.isDirectory()) {
                syncDir = new File("SyncDir\\");
                if (!syncDir.exists()) {
                    syncDir.mkdir();
                }
                writeDefaultConfig();
                System.out.println("Directory inesistente\nCartella di sincronizzazione di default impostata");
            }
        }

        reader.close();
    }

    /**
     * Metodo per il trasferimento di un pezzo di file
     *
     * @param fhs il file in cui inserire il nuovo pezzo
     * @param index l'indice del pezzo
     * @throws IOException se ci sono errori di lettura file
     * @throws MyExc se il trasferimento non va a buon fine
     */
    public static void ChunkTransfer(FileHandlerServer fhs, int index) throws IOException, MyExc {
        logger.log("Sending request");
        chunkReq.newBuilder()
                .setCrc(fhs.aiaOld.array[index])
                .setInd(index)
                .setNam(fhs.ServerFile.getName())
                .build().writeDelimitedTo(socket.getOutputStream());
        logger.log("Waiting for response");
        info infoChunkPacket = info.parseDelimitedFrom(socket.getInputStream());
        logger.log("Got response");
        fhs.setChunkToRecv((int) infoChunkPacket.getLen());
        fhs.getChunkInfoRespPacket().writeDelimitedTo(socket.getOutputStream());

        int i = 0, ErrorCount = 0;
        for (; i < fhs.nChunkPackets; i++) {
            logger.log("Downloading packet n." + i);
            data dataPacket = data.parseDelimitedFrom(socket.getInputStream());
            logger.log("Download completed");

            resp respPacket = fhs.addChunkPacket(dataPacket, i);
            respPacket.writeDelimitedTo(socket.getOutputStream());
            if (respPacket.getRes().equals("mrr")) {
                break;
            }
        }
        resp respEndPacket = resp.parseDelimitedFrom(socket.getInputStream());
        if (respEndPacket.getRes().equals("not") && ErrorCount < 3) {
            i--;
            ErrorCount++;
        } else if (respEndPacket.getRes().equals("not") && ErrorCount >= 3) {
            throw new MyExc("Errore di trasferimento");
        } else if (respEndPacket.getRes().equals("ok")) {
            ErrorCount = 0;
            if (i == fhs.nChunkPackets) {
                logger.log("Download finished");
                fhs.insertChunk(index);
                logger.log("Inserted chunk into position " + index);
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Thread thCmd = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        String command = new BufferedReader(new InputStreamReader(System.in)).readLine();
                        if (command.equals("exit") || command.equals("close")) {
                            System.exit(0);
                        } else if (!command.equals("")) {
                            System.err.println("'" + command + "' is not a command");
                        }
                    } catch (IOException ex) {
                    }
                }
            }
        });
        thCmd.start();

        while (true) {
            try {
                System.out.println("Server is listening | Port: 6365");
                ss = new ServerSocket(6365);
                socket = ss.accept();
                logger = new Logger();
                System.out.println("Connected to: " + socket.getRemoteSocketAddress());
                Logger.log("Connected to: " + socket.getRemoteSocketAddress());
                if (!new File("sConfig.ini").exists()) {
                    writeDefaultConfig();
                }
                readConfig();

                System.out.println("Waiting for synchronization...");
                crcInfo CRCInfoPacket = crcInfo.parseDelimitedFrom(socket.getInputStream());
                System.out.println("Synchronization started...");
                Logger.log("Synchronization started");
                Files = syncDir.listFiles();
                ChunkSize = CRCInfoPacket.getCsz();
                SyncFiles = new FileHandlerServer[CRCInfoPacket.getCrcCount()];
                try {
                    for (int i = 0; i < CRCInfoPacket.getCrcCount(); i++) {
                        int j = 0;
                        for (; j < Files.length; j++) {
                            if (CRCInfoPacket.getCrc(i).equals(Files[j].getName())) {
                                SyncFiles[i] = new FileHandlerServer(Files[j], CRCInfoPacket.getLen(i), CRCInfoPacket.getCln(i), ChunkSize);
                                break;
                            }
                        }
                        if (j == Files.length) {
                            if (new File("Indexes\\" + CRCInfoPacket.getCrc(i) + ".crc").exists()) {
                                new File("Indexes\\" + CRCInfoPacket.getCrc(i) + ".crc").delete();
                            }
                            SyncFiles[i] = new FileHandlerServer(new File(syncDir.getName() + "\\" + CRCInfoPacket.getCrc(i)), CRCInfoPacket.getLen(i), CRCInfoPacket.getCln(i), ChunkSize);
                        }
                        if (!SyncFiles[i].oldCRCIndex.exists() || SyncFiles[i].oldVersion != CRCInfoPacket.getVer(i)) {
                            SyncFiles[i].getNewCRCRequest(CRCInfoPacket.getVer(i)).writeDelimitedTo(socket.getOutputStream());
                            System.out.println("Downloading " + SyncFiles[i].newCRCIndex.getName());
                            logger.log("Downloading " + SyncFiles[i].newCRCIndex.getName());
                                    
                            info newCRCInfoPacket = info.parseDelimitedFrom(socket.getInputStream());
                            logger.log("Got info packet");
                            //System.out.println(newCRCInfoPacket.toString());
                            if (newCRCInfoPacket.getVer() == CRCInfoPacket.getVer(i)) {
                                SyncFiles[i].getCRCIndexInfoRespPacket().writeDelimitedTo(socket.getOutputStream());
                                int k = 0, ErrorCount = 0;
                                for (; k < SyncFiles[i].nCRCIndexPackets; k++) {
                                    data dataPacket = data.parseDelimitedFrom(socket.getInputStream());
                                    resp respPacket = SyncFiles[i].addCRCIndexPacket(dataPacket, k);
                                    respPacket.writeDelimitedTo(socket.getOutputStream());
                                    if (respPacket.getRes().equals("mrr")) {
                                        break;
                                    }
                                }
                                resp respEndPacket = resp.parseDelimitedFrom(socket.getInputStream());
                                System.out.println(respEndPacket.toString());
                                if (respEndPacket.getRes().equals("not") && ErrorCount < 3) {
                                    i--;
                                    ErrorCount++;
                                } else if (respEndPacket.getRes().equals("not") && ErrorCount >= 3) {
                                    throw new MyExc("Errore di trasferimento");
                                } else if (respEndPacket.getRes().equals("ok")) {
                                    ErrorCount = 0;
                                    if (k == SyncFiles[i].nCRCIndexPackets) {
                                        SyncFiles[i].readNewDigests();
                                    }
                                }
                            }
                        } else if (SyncFiles[i].oldCRCIndex.exists() && SyncFiles[i].oldVersion == CRCInfoPacket.getVer(i)) {
                            SyncFiles[i].newVersion = CRCInfoPacket.getVer(i);
                        }
                    }
                    crcReq.newBuilder().setCrc("end").build().writeDelimitedTo(socket.getOutputStream());
                    resp endResponse = resp.parseDelimitedFrom(socket.getInputStream());
                    System.out.println(endResponse.toString());
                    if (!endResponse.getRes().equals("ok")) {
                        throw new MyExc("Server error");
                    }

                    int ChunksToDownload = 0;
                    for (FileHandlerServer fhs : SyncFiles) {
                        if (fhs.oldVersion != fhs.newVersion) {
                            ChunksToDownload += fhs.countChunksToDownload();
                        }
                    }
                    resp.newBuilder()
                            .setInd(ChunksToDownload)
                            .build()
                            .writeDelimitedTo(socket.getOutputStream());
                    resp startResp = resp.parseDelimitedFrom(socket.getInputStream());

                    for (FileHandlerServer fhs : SyncFiles) {
                        if (fhs.oldVersion != fhs.newVersion) {
                            fhs.compareIndexes();

                            System.out.println("\n" + fhs.ServerFile.getName() + " handling started...");
                            Logger.log(fhs.ServerFile.getName() + " handling started");
                            // inizio delle operazioni di aggiornamento del vecchio file
                            if (fhs.oldChunks < fhs.newChunks) {
                                long[] newArray = new long[(int) fhs.newChunks];
                                System.arraycopy(fhs.aiaOld.array, 0, newArray, 0, fhs.aiaOld.array.length);
                                fhs.aiaOld.array = newArray;
                                System.out.println("\nNew version is bigger");
                                Logger.log("New version is bigger");
                                for (int i = fhs.aiaOld.sIndexes.length; i < fhs.aiaOld.array.length; i++) {
                                    int j;
                                    ByteBuffer buf = ByteBuffer.allocate(ChunkSize);
                                    for (j = 0; j < fhs.aiaOld.sIndexes.length; j++) {
                                        int index = fhs.aiaOld.searchColumnIndex(j, i);
                                        if (index != -1) {
                                            int len;
                                            if ((len = fhs.fcServerRead.read(buf, (long) j * ChunkSize)) != -1) {
                                                fhs.ChunkToRecv = fhs.getByteArray(buf);
                                                fhs.insertChunk(i);
                                                //System.out.println("Copied chunk " + j + " to " + i);
                                                Logger.log("Copied chunk " + j + " to " + i);
                                                fhs.aiaOld.array[i] = fhs.aiaOld.array[j];
                                                if (fhs.aiaOld.sIndexes[j].length == 1) {
                                                    fhs.aiaOld.sIndexes[j][0] = -1;
                                                } else {
                                                    fhs.aiaOld.delIndex(j, index);
                                                }
                                                break;
                                            }
                                        }
                                    }
                                    if (j == fhs.aiaOld.sIndexes.length) {
                                        //System.out.println("Downloading chunk n." + i + "...");
                                        Logger.log("Downloading chunk n." + i);
                                        ChunkTransfer(fhs, i);
                                        //System.out.println("Finished");
                                        //System.out.println("Copied chunk " + j + " from new version to " + i);
                                        fhs.aiaOld.array[i] = fhs.iaNew.array[i];
                                    }
                                }
                            }
                            long dBuf = 0;
                            int[] dsIndexes = null;
                            byte[] dChunk = null;
                            int iterations;
                            if (fhs.newChunks >= fhs.oldChunks) {
                                iterations = fhs.aiaOld.sIndexes.length;
                            } else {
                                iterations = (int) fhs.newChunks;
                            }
                            System.out.println("Local file processing started...");
                            Logger.log("Local file processing started");
                            while (true) {
                                int updated = 0, notUp = 0;
                                ByteBuffer buf = ByteBuffer.allocate(ChunkSize);
                                for (int i = 0; i < iterations; i++) {
                                    if (fhs.aiaOld.sIndexes[i][0] == -1) {
                                        //System.out.println("Processing chunk " + i);
                                        logger.log("Processing chunk " + i);
                                        int k = -1;
                                        if (dsIndexes != null && dChunk != null) {
                                            for (k = 0; k < dsIndexes.length; k++) {
                                                if (dsIndexes[k] == i) {
                                                    fhs.fcServerWrite.write(ByteBuffer.wrap(dChunk), (long) i * ChunkSize);
                                                    //System.out.println("Copied buffer to " + i);
                                                    logger.log("Copied buffer to position " + i);
                                                    fhs.aiaOld.array[i] = dBuf;
                                                    fhs.aiaOld.sIndexes[i][0] = i;
                                                    if (dsIndexes.length > 1) {
                                                        for (int z = k; z < dsIndexes.length - 1; z++) {
                                                            dsIndexes[z] = dsIndexes[z + 1];
                                                        }
                                                    } else {
                                                        dsIndexes = null;
                                                        dBuf = 0;
                                                        dChunk = null;
                                                    }
                                                    updated++;
                                                    break;
                                                }
                                            }

                                            if (dsIndexes != null && k == dsIndexes.length) {
                                                k = -1;
                                            }
                                        }
                                        if (k == -1) {
                                            int j = 0;
                                            for (; j < fhs.aiaOld.sIndexes.length; j++) {
                                                int index = fhs.aiaOld.searchColumnIndex(j, i);
                                                if (index != -1) {
                                                    int len;
                                                    if ((len = fhs.fcServerRead.read(buf, (long) j * ChunkSize)) != -1) {
                                                        fhs.ChunkToRecv = fhs.getByteArray(buf);
                                                        //fOutOld.write(ByteBuffer.wrap(chunk), (long) i * ChunkSize);
                                                        fhs.insertChunk(i);
                                                        //System.out.println("Copied chunk " + j + " to " + i);
                                                        logger.log("Copied chunk " + j + " to " + i);
                                                        fhs.aiaOld.array[i] = fhs.aiaOld.array[j];
                                                        fhs.aiaOld.sIndexes[i][0] = i;
                                                        if (fhs.aiaOld.sIndexes[j].length == 1) {
                                                            fhs.aiaOld.sIndexes[j][0] = -1;
                                                            i = j - 1;
                                                        } else {
                                                            fhs.aiaOld.delIndex(j, index);
                                                        }
                                                        updated++;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (j == fhs.aiaOld.sIndexes.length) {
                                                //int len;
                                                //if ((len = fNew.read(buf, (long) i * ChunkSize)) != -1) {
                                                ChunkTransfer(fhs, i);
                                                //System.out.println("Copied chunk " + i + " from new version");
                                                fhs.aiaOld.array[i] = fhs.iaNew.array[i];
                                                fhs.aiaOld.sIndexes[i][0] = i;
                                                updated++;
                                                //}
                                            }
                                        }
                                    } else {
                                        if (fhs.aiaOld.searchColumnIndex(i, i) != -1) {
                                            updated++;
                                            notUp++;
                                        } else {
                                            notUp++;
                                        }
                                    }
                                }
                                if (updated == iterations) {
                                    break;
                                }
                                if (notUp == iterations) {
                                    for (int i = 0; i < fhs.aiaOld.sIndexes.length; i++) {
                                        if (fhs.aiaOld.searchColumnIndex(i, i) == -1 && fhs.aiaOld.sIndexes[i][0] != -1) {
                                            int len;
                                            if ((len = fhs.fcServerRead.read(buf, (long) i * ChunkSize)) != -1) {
                                                dChunk = fhs.getByteArray(buf);
                                                dBuf = fhs.aiaOld.array[i];
                                                dsIndexes = new int[fhs.aiaOld.sIndexes[i].length];
                                                System.arraycopy(fhs.aiaOld.sIndexes[i], 0, dsIndexes, 0, fhs.aiaOld.sIndexes[i].length);
                                                fhs.aiaOld.sIndexes[i] = new int[]{-1};
                                            }
                                        }
                                    }
                                }
                            }
                            if (fhs.ServerFile.length() > fhs.newLength) {
                                fhs.fcServerWrite.truncate(fhs.newLength);
                                System.out.println("New file is smaller -> Old file truncated to " + fhs.newLength);
                                Logger.log("New file is smaller -> Old file truncated to " + fhs.newLength);
                                long[] newArray = new long[(int) fhs.newChunks];
                                System.arraycopy(fhs.aiaOld.array, 0, newArray, 0, (int) fhs.newChunks);
                            }

                            fhs.writeDigests();
                            fhs.newCRCIndex.delete();
                        }
                    }
                    chunkReq.newBuilder().setInd(-1).build().writeDelimitedTo(socket.getOutputStream());
                } catch (IOException | NumberFormatException | MyExc ex) {
                    System.out.println("Error: " + ex.getMessage());
                    Logger.log("Error: " + ex.getMessage());
                    if (RetryCount < 3) {
                        RetryCount++;
                    } else {
                        break;
                    }
                }

                if (delete) {
                    for (File f : Files) {
                        int i = 0;
                        for (; i < SyncFiles.length; i++) {
                            if (f.getName().equals(SyncFiles[i].ServerFile.getName())) {
                                break;
                            }
                        }
                        if (i == SyncFiles.length) {
                            f.delete();
                            logger.log("File " + f.getName() + "deleted");
                        }
                    }
                }

                System.out.println("Synchronization completed");
                Logger.log("Synchronization completed");
                
                socket.close();
                ss.close();
            } catch (IOException ex) {
                System.out.println("Error: " + ex.getMessage());
                Logger.log("Error: " + ex.getMessage());
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException exc) {
                    }
                }
                if (ss != null) {
                    try {
                        ss.close();
                    } catch (IOException exc) {
                    }
                }
                if (RetryCount < 3) {
                    RetryCount++;
                } else {
                    System.exit(0);
                }
            }
            for (FileHandlerServer fhs : SyncFiles) {
                try {
                    fhs.fcServerRead.close();
                    fhs.fcServerWrite.close();
                    fhs.ServerFile = null;
                    fhs.newCRCIndex = null;
                    fhs.oldCRCIndex = null;
                    fhs = null;
                } catch (IOException ex) {
                    System.out.println("Error: " + ex.getMessage());
                    Logger.log("Error: " + ex.getMessage());
                }
            }
            SyncFiles = null;
            for (File f : Files) {
                f = null;
            }
            Files = null;
            
            logger.closeLogger();
        }
    }

}

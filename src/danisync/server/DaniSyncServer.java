/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package danisync.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Stefano Fiordi
 */
public class DaniSyncServer {

    static File syncDir;
    static int ChunkSize;
    static File Files[];

    /**
     * Scrive la configurazione del server sul file sConfig.ini
     *
     * @throws IOException se ci sono errori durante la scrittura
     */
    public static void writeDefaultConfig() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("sConfig.ini"), false));
        String s = "Path = SyncDir\\";
        writer.write(s);

        writer.close();
    }

    /**
     * Legge la configurazione del server dal file sConfig.ini
     *
     * @throws IOException se ci sono errori durante la lettura
     * @throws MyExc se la versione non viene letta correttamente
     */
    public static void readConfig() throws IOException, MyExc {
        BufferedReader reader = new BufferedReader(new FileReader(new File("sConfig.ini")));
        String[] line = reader.readLine().split(" = ");
        syncDir = new File(line[1]);
        if (!syncDir.exists() || !syncDir.isDirectory()) {
            syncDir = new File("SyncDir\\");
            if (!syncDir.exists()) {
                syncDir.mkdir();
            }
            writeDefaultConfig();
            throw new MyExc("Directory inesistente\nCartella di sincronizzazione di default impostata");
        }

        reader.close();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            if (!new File("sConfig.ini").exists()) {
                writeDefaultConfig();
            }
            readConfig();
            Files = syncDir.listFiles();
        } catch (IOException | MyExc ex) {
            System.out.println("Error: " + ex.getMessage());
        }

        while (true) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (new BufferedReader(new InputStreamReader(System.in)).readLine().equals("close")) {
                            System.exit(0);
                        }
                    } catch (IOException ex) {
                    }
                }
            }).start();

            try {
                ServerSocket ss = new ServerSocket(6365);
                Socket socket = ss.accept();

            } catch (IOException ex) {
                System.out.println("Error: " + ex.getMessage());
            }
        }
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package danisync.server;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author Stefano Fiordi
 */
public class DaniSyncServer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        File oldVersion = new File("E:/vdis/FSV.vdi"); // vacchia versione del file da aggiornare
        final int ChunkSize = 1024 * 1024; // grandezza dei pezzi di file
        long oldChunks, ver;
        long[] oldDigests;
        
        
        try {
            ServerSocket ss = new ServerSocket(6365);
            Socket socket = ss.accept();
                        
        } catch (IOException ex) {
            System.out.println("Error: " + ex.getMessage());
        }
    }
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dani3la.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Classe che si occupa del logging delle attività del server
 *
 * @author Stefano Fiordi
 */
public class Logger {

    /**
     * Il logger che scriverà su un file di log le operazioni del server
     */
    private static BufferedWriter logger;

    /**
     * Costruttore che inizializza il logger creando un nuovo file di log ed
     * inizializzando il canale di scrittura
     */
    public Logger() {
        if (!new File("Logs\\").exists()) {
            new File("Logs\\").mkdir();
        }
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");
            logger = new BufferedWriter(new FileWriter(new File("Logs\\" + LocalDateTime.now().format(formatter) + "_server.log"), true));
        } catch (IOException ex) {
            System.out.println("Unable to initialize logger: " + ex.getMessage());
        }
    }

    /**
     * Metodo che scrive sul file di log la stringa s
     *
     * @param s la stringa da riportare sul file di log
     */
    public static void log(String s) {
        if (logger != null) {
            try {
                logger.write("[" + LocalDateTime.now() + "] " + s);
                logger.flush();
                logger.newLine();
            } catch (IOException ex) {
                System.out.println("Logger error");
            }
        }
    }

    /**
     * Metodo che chiude il canale di scrittura del logger
     */
    public void closeLogger() {
        if (logger != null) {
            try {
                logger.close();
            } catch (IOException ex) {
                System.out.println("Unable to close logger");
            }
        }
    }
}

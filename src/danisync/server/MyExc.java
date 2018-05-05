/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package danisync.server;

/**
 *
 * @author Stefano Fiordi
 */
public class MyExc extends Exception {

    /**
     * Constructs an instance of <code>MyExc</code> with the specified detail
     * message.
     *
     * @param msg the detail message.
     */
    public MyExc(String msg) {
        super(msg);
    }
}

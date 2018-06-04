/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dani3la.server;

/**
 * Classe per la gestione di un array di digest in base ad un array di indici
 * secondari
 *
 * @author Stefano Fiordi
 */
public class IndexedArray {

    protected long[] array; // array di digest
    protected int[] sIndexes; // array di indici secondari

    /**
     * Costruttore che inizializza l'array di indici secondari
     *
     * @param array array di digest
     */
    public IndexedArray(long[] array) {
        this.array = array;
        this.sIndexes = new int[array.length];
    }

}


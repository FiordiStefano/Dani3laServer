/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package danisync.server;

/**
 * Classe per la gestione di un array di digest in base ad un array
 * bidimensionale di indici secondari
 *
 * @author Stefano Fiordi
 */
public class AIndexedArray {

    protected long[] array; // array di digest
    protected int[][] sIndexes; // array bidimensionale di indici secondari

    /**
     * Costruttore che inizializza l'array bidimensionale in base alla lunghezza
     * dell'array di digest
     *
     * @param array array di digest
     */
    public AIndexedArray(long[] array) {
        this.array = array;
        this.sIndexes = new int[array.length][];
        for (int i = 0; i < sIndexes.length; i++) {
            sIndexes[i] = new int[0];
        }
    }

    /**
     * Metodo per la riallocazione di un array dell'array bidimensionale ad una
     * nuova lunghezza
     *
     * @param row la riga dell'array bidimensionale
     * @param newLength nuova lunghezza dell'array
     * @throws MyExc se la nuova lunghezza e/o la riga non sono valide
     */
    private void realloc(int row, int newLength) throws MyExc {
        if (row >= array.length || row < 0) {
            throw new MyExc("Invalid index");
        }
        if (newLength < 0) {
            throw new MyExc("Invalid new length");
        }
        int[] newIndexes = new int[newLength];
        if (newLength > sIndexes[row].length) {
            System.arraycopy(sIndexes[row], 0, newIndexes, 0, sIndexes[row].length);
        } else {
            System.arraycopy(sIndexes[row], 0, newIndexes, 0, newLength);
        }
        sIndexes[row] = newIndexes;
    }

    /**
     * Metodo per l'aggiunta di un indice all'array bidimensionale alla data
     * riga
     *
     * @param row la riga dell'array bidimensionale
     * @param value l'indice da inserire
     * @throws MyExc se l'indice non è valido
     */
    protected void addIndex(int row, int value) throws MyExc {
        if (value < -1) {
            throw new MyExc("Invalid new index value");
        }
        realloc(row, sIndexes[row].length + 1);
        sIndexes[row][sIndexes[row].length - 1] = value;
    }

    /**
     * Metodo per la rimozione di un indice dall'array bidimensionale
     *
     * @param row la riga dell'array bidimensionale
     * @param col la colonna dell'array bidimensionale
     * @throws MyExc se la riga e/o la colonna non sono valide
     */
    protected void delIndex(int row, int col) throws MyExc {
        if (row >= array.length || row < 0 || col >= array.length || col < 0) {
            throw new MyExc("Invalid index");
        }
        for (int i = col; i < sIndexes[row].length - 1; i++) {
            sIndexes[row][i] = sIndexes[row][i + 1];
        }
        realloc(row, sIndexes[row].length - 1);
    }

    /**
     * Metodo che ricerca un indice nell'array bidimensionale alla data riga
     * @param row la riga dell'array bidimensionale
     * @param searchIndex l'indice da cercare
     * @return la colonna corrispondente all'indice se trovato, altrimenti -1
     */
    protected int searchColumnIndex(int row, int searchIndex) {
        for (int i = 0; i < sIndexes[row].length; i++) {
            if (sIndexes[row][i] == searchIndex) {
                return i;
            }
        }

        return -1;
    }
}

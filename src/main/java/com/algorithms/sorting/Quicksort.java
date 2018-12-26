package com.algorithms.sorting;

import com.util.io.Console;

public class Quicksort {
    public static void main(String args[]) {
        Console objConsole = new Console();
        String[] strArrayInput = objConsole.fnReadArrayfromConsole();
        int[] intArrayInput = new int[strArrayInput.length];
        for (int i = 0; i < strArrayInput.length; i++) {
            intArrayInput[i] = Integer.parseInt(strArrayInput[i]);
            System.out.print(intArrayInput[i] + ";");
        }
        System.out.println("");
    }
}

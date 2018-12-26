package com.algorithms.sorting;

import com.util.io.Console;


public class Bubblesort {
    public static void main(String args[]) {
        Console objConsole = new Console();
        String[] strArrayInput = objConsole.fnReadArrayfromConsole();
        int[] intArrayInput = new int[strArrayInput.length];
        for (int i = 0; i < strArrayInput.length; i++) {
            intArrayInput[i] = Integer.parseInt(strArrayInput[i]);
            System.out.print(intArrayInput[i] + ";");
        }
        System.out.println("");
        for (int i = 0; i < intArrayInput.length; i++) {
            for (int j = 0; j < intArrayInput.length-i-1; j++) {
                if (intArrayInput[j] > intArrayInput[j+1]) {
                    int intTemp = intArrayInput[j];
                    intArrayInput[j] = intArrayInput[j+1];
                    intArrayInput[j+1] = intTemp;
                }
            }
        }

        for (int i = 0; i < intArrayInput.length; i++) {
            System.out.print(intArrayInput[i] + ";");
        }
    }
}

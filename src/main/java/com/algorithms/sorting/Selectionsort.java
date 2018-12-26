package com.algorithms.sorting;

import com.util.io.Console;

public class Selectionsort {

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
            int intSmallestValue = intArrayInput[i];
            int intSmallestValuePosition = i;
            for (int j = i + 1; j < intArrayInput.length; j++) {
                if (intSmallestValue > intArrayInput[j]) {
                    intSmallestValue = intArrayInput[j];
                    intSmallestValuePosition = j;
                }
            }
            if (intSmallestValuePosition != i) {
                int intTemp = intArrayInput[i];
                intArrayInput[i] = intArrayInput[intSmallestValuePosition];
                intArrayInput[intSmallestValuePosition] = intTemp;
            }
        }

        for (int i = 0; i < intArrayInput.length; i++) {
            System.out.print(intArrayInput[i] + ";");
        }
    }
}

package com.algorithms.sorting;


import com.util.io.Console;

public class Insertionsort {
    public static void main(String args[]) {
        Console objConsole = new Console();
        String[] strArrayInput = objConsole.fnReadArrayfromConsole();
        int intBiggestNumber = 0;
        for (int i = 0; i < strArrayInput.length; i++) {
            if (i == 0) {
                intBiggestNumber = Integer.parseInt(strArrayInput[0]);
                continue;
            } else if (Integer.parseInt(strArrayInput[i]) > intBiggestNumber) {
                intBiggestNumber = Integer.parseInt(strArrayInput[i]);
                continue;
            }
            int j = i - 1;
            int k = i;
            String strTemp = "";
            while (j >= 0 && Integer.parseInt(strArrayInput[j]) > Integer.parseInt(strArrayInput[k]) ) {
                strTemp = strArrayInput[j];
                strArrayInput[j] = strArrayInput[k];
                strArrayInput[k] = strTemp;
                j--;
                k--;
            }
        }
        objConsole.fnPrintArray(strArrayInput);
    }
}

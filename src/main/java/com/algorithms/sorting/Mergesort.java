package com.algorithms.sorting;

import com.util.io.Console;

public class Mergesort {
    public static void main(String args[]) {
        Console objConsole = new Console();
        String[] strArrayInput = objConsole.fnReadArrayfromConsole();
        int[] intArrayInput = new int[strArrayInput.length];
        for (int i = 0; i < strArrayInput.length; i++) {
            intArrayInput[i] = Integer.parseInt(strArrayInput[i]);
            System.out.print(intArrayInput[i] + ";");
        }
        System.out.println("");
        int intArraysize = intArrayInput.length;
        for (int size = 1; size < intArraysize; size *= 2) {
            for (int start = 0; start < intArraysize; start += 2 * size) {
                int end = Math.min(start + 2 * size - 1, intArraysize - 1);
                int mid = Math.min(start + size - 1, intArraysize - 1);
                merge(intArrayInput, start, mid, end);
            }
        }

        for (int i = 0; i < strArrayInput.length; i++) {
            System.out.print(intArrayInput[i] + ";");
        }
    }

    public static void merge(int arr[], int start, int mid, int end) {
        int i, j, k;
        int n1 = mid - start + 1;
        int n2 = end - mid;

        int L[] = new int[n1], R[] = new int[n2];
        for (i = 0; i < n1; i++)
            L[i] = arr[start + i];
        for (j = 0; j < n2; j++)
            R[j] = arr[mid + 1 + j];

        i = 0;
        j = 0;
        k = start;
        while (i < n1 && j < n2) {
            if (L[i] <= R[j]) {
                arr[k] = L[i];
                i++;
            } else {
                arr[k] = R[j];
                j++;
            }
            k++;
        }

        while (i < n1) {
            arr[k] = L[i];
            i++;
            k++;
        }

        while (j < n2) {
            arr[k] = R[j];
            j++;
            k++;
        }
    }

}

package com.util.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Console {
    public String[] fnReadArrayfromConsole() {
        BufferedReader objBR = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter the input separated with semicolon:");
        String strValue = "";
        try {
            strValue = objBR.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return strValue.split(";");
    }

    public void fnPrintArray(String[] args) {
        for (String s : args) {
            System.out.println(s);
        }

    }
}

package com.kkap;

public class Main {
    public static void main(String[] args) throws Exception {
        int level = Integer.parseInt(args[args.length - 1]);
        switch (level) {
            case 1:
                WordCount_Level1.execute(args);
                break;
            case 2:
                WordCount_Level2.execute(args);
                break;
            case 3:
                WordCount_Level3.execute(args);
                break;
        }
    }
}

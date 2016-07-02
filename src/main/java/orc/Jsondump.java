package orc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.FileDump;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
/**
 * Created by tomk on 3/5/16.
 * Adapted by Wendy Wong in June/16.
 * Basically, trying to play with and understand the Orc Java class, what I can get out of it.
 */
public class Jsondump {
    public static void main(String[] args) throws IOException {
        long tempCount = 0;
        Path testFilePath = new Path("/Users/wendycwong/orc-reader-example/smalldata/parser/orc/decimal.orc");


        PrintStream origOut = System.out;
        String outputFilename = "orc-file-dump.json";
        FileOutputStream myOut = new FileOutputStream("/Users/wendycwong/orc-reader-example/smalldata/parser/orc/decimal-dump.json");

        // replace stdout and run command
        System.setOut(new PrintStream(myOut));
        try {
            FileDump.main(new String[]{testFilePath.toString(), "-j", "-p", "--rowindex=3"});
    //        FileDump.main(new String[]{testFilePath.toString(), "-j", "-p", "-d"});
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.flush();
        System.setOut(origOut);
    }
}
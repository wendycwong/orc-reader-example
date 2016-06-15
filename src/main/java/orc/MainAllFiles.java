
package orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.conf.Configuration;
import java.io.File;

import java.net.URI;


/* This main will go through all files in the directory and I am trying
   to find orc files with more than one column in it.
 */
public class MainAllFiles {
    public static void main(String[] argv) throws Exception {
        FileSystem fs = new LocalFileSystem();
        Configuration mainConf = new Configuration();
        fs.initialize(new URI("file://localhost"), mainConf);
        String fileName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/float_single_col.orc";
        File folder = new File("/Users/wendycwong/orc-reader-example/smalldata/parser/orc");
        File[] listOfFiles = folder.listFiles();

//    MyOrc m = new MyOrc();
//    long rowCount = m.getRowCount(fs, fileName);

        WendyOrc wm = new WendyOrc();

        for (File f:listOfFiles) {
            long rowCountw = wm.getRowCount(fs, f.getAbsoluteFile().toString());
            System.out.println("rowCount is " + rowCountw);
        }


    }
}
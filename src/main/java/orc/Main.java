
package orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class Main {
  public static void main(String[] argv) throws Exception {
    FileSystem fs = new LocalFileSystem();
    Configuration mainConf = new Configuration();
    fs.initialize(new URI("file://localhost"), mainConf);
    String fileName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/float_single_col.orc";

//    MyOrc m = new MyOrc();
//    long rowCount = m.getRowCount(fs, fileName);

    WendyOrc wm = new WendyOrc();
    long rowCountw = wm.getRowCount(fs, fileName);

    System.out.println("rowCount is " + rowCountw);
  }
}


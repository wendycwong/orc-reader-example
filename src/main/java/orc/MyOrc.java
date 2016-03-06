package orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Created by tomk on 3/5/16.
 */
public class MyOrc {
  public long getRowCount(FileSystem fs, String fName) throws Exception {
    long tempCount = 0;
    Path p = new Path(fName);
    Reader rdr = OrcFile.createReader(fs, p);
    StructObjectInspector insp = (StructObjectInspector) rdr.getObjectInspector();
    Iterable<StripeInformation> iterable = rdr.getStripes();
    for(StripeInformation stripe:iterable){
      tempCount = tempCount + stripe.getNumberOfRows();
    }
    return tempCount;
  }
}

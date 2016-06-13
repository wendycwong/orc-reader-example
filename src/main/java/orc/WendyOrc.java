package orc;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.nio.ByteBuffer;
import java.util.List;


/**
 * Created by tomk on 3/5/16.
 */
public class WendyOrc {
    public long getRowCount(FileSystem fs, String fName) throws Exception {
        long tempCount = 0;
        Path p = new Path(fName);
        Reader rdr = OrcFile.createReader(fs, p);

        CompressionKind ckind = rdr.getCompression();   // get compression kind, SNAPPY, or others
        int csze = rdr.getCompressionSize();

        List<org.apache.hadoop.hive.ql.io.orc.OrcProto.Type> ftypes = rdr.getTypes();

        StructObjectInspector insp = (StructObjectInspector) rdr.getObjectInspector();

        Iterable<String> mdatakey = rdr.getMetadataKeys();  // try to get user meta data
        for (String mkey:rdr.getMetadataKeys()) {
            ByteBuffer mddata = rdr.getMetadataValue(mkey);
        }

        ColumnStatistics[] mstat = rdr.getStatistics(); // get column statistic, count, min, max, sum,

        long fileNumberOfRows = rdr.getNumberOfRows();   // number of rows in whole file

        boolean[] toInclude = {true,true};      // array must equal to number of column types
        RecordReader scanEntireFile = rdr.rows(toInclude);     // RecordReader that will scan entire file
        long fileSize = rdr.getContentLength();         // get the length of file in bytes
        Iterable<StripeInformation> iterable = rdr.getStripes();
        for(StripeInformation stripe:iterable){
            tempCount = tempCount + stripe.getNumberOfRows();
        }
        return tempCount;
    }
}
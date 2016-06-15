package orc;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;


/**
 * Created by tomk on 3/5/16.
 * Adapted by Wendy Wong in June/16.
 * Basically, trying to play with and understand the Orc Java class, what I can get out of it.
 */
public class WendyOrc {
    public long getRowCount(FileSystem fs, String fName) throws Exception {
        long tempCount = 0;
        Path p = new Path(fName);
        Reader rdr = OrcFile.createReader(fs, p);   // must createReader before accessing file contents.

        CompressionKind ckind = rdr.getCompression();   // get compression kind, SNAPPY, or others
        int csze = rdr.getCompressionSize();

        System.out.println("Compression kind is "+ckind);
        System.out.println("Compression size is "+csze);

        List<org.apache.hadoop.hive.ql.io.orc.OrcProto.Type> ftypes = rdr.getTypes();

        StructObjectInspector insp = (StructObjectInspector) rdr.getObjectInspector();
        List<StructField> allColInfo = (List<StructField>) insp.getAllStructFieldRefs();

        for (StructField oneField:allColInfo) {
            int fieldID = oneField.getFieldID();
            String fieldName = oneField.getFieldName();
        }

        Iterable<String> mdatakey = rdr.getMetadataKeys();  // try to get user meta data
        for (String mkey:rdr.getMetadataKeys()) {
            ByteBuffer mddata = rdr.getMetadataValue(mkey);
        }

        ColumnStatistics[] mstat = rdr.getStatistics(); // get column statistic, count, min, max, sum,

        long fileNumberOfRows = rdr.getNumberOfRows();   // number of rows in whole file
        int numberOfColumns = ftypes.size();

        RecordReader scanFile = rdr.rows();
        boolean[] toInclude = new boolean[numberOfColumns];      // array must equal to number of column types
        Arrays.fill(toInclude, true);
        RecordReader scanEntireFile = rdr.rows(toInclude);     // RecordReader that will scan entire file
        OrcStruct what = (OrcStruct) scanEntireFile.next(null);

//        while (scanEntireFile.hasNext()) {

//            for (int index = 0; index < numberOfColumns-1; index++) {
//                Object val = ((OrcStruct) what).getFieldValue(index);
//                System.out.println("Column index: "+index+". Value is "+val.toString());
//            }

//            what = (OrcStruct) scanEntireFile.next(what);

//        }





        long fileSize = rdr.getContentLength();         // get the length of file in bytes
        List<StripeInformation> stripesInfo = rdr.getStripes();
        for (StripeInformation oneStripe:stripesInfo) {
            long rowCount = oneStripe.getNumberOfRows();
            long offset = oneStripe.getOffset();
            long stipeSizeByte = oneStripe.getDataLength();

        }

        Iterable<StripeInformation> iterable = rdr.getStripes();
      //  int[] stripeRowNumber = new int[];
        long stripeSizes = 0;
        for(StripeInformation stripe:iterable){
            tempCount = tempCount + stripe.getNumberOfRows();
            stripeSizes = stripeSizes + stripe.getDataLength();
        }
        return tempCount;
    }
}
package orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.net.URI;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by tomk on 3/5/16.
 * Adapted by Wendy Wong in June/16.
 * Basically, trying to play with and understand the Orc Java class, what I can get out of it.
 * This sample code is copied from https://orc.apache.org/docs/core-java.html.
 */
public class orcReader_Orc_Core {
    public static void main(String[] argv) throws Exception {

        FileSystem fs = new LocalFileSystem();
        Configuration mainConf = new Configuration();
        fs.initialize(new URI("file://localhost"), mainConf);
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/demo-11-zlib.orc";
    //    String fName = "/Users/wendycwong/h2o-3/smalldata/parser/avro/episodes.avro";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/bool_single_col.orc";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/bigint_single_col.orc";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/TestOrcFile.testDate1900.orc";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/string_single_col.orc";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/decimal.orc";
        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/TestOrcFile.testStringAndBinaryStatistics.orc";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/TestOrcFile.testTimestamp.orc";
//        String fName = "/Users/wendycwong/orc-reader-example/smalldata/parser/orc/TestOrcFile.testDate2038.orc";


        DateFormat dataFormat = new SimpleDateFormat("dd/MM/yyyy");
        Date data = dataFormat.parse("01/01/1970");
        long temptime = data.getTime();
        Timestamp myTime = new Timestamp(temptime);
 //       Timestamp time2 = Timestamp.valueOf("1970-01-01");

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.setTime(dataFormat.parse("01/01/1970"));
        cal.getTimeInMillis();

        Path p = new Path(fName);
        Reader reader = OrcFile.createReader(fs, p);   // must createReader before accessing file contents.
        Reader reader2 = OrcFile.createReader(new Path(fName), OrcFile.readerOptions(mainConf));
        int totalRows3 = (int) reader2.getNumberOfRows();

//        WritableTimestampObjectInspector insp2 = (WritableTimestampObjectInspector) reader.getObjectInspector();


        List<StripeInformation> stripesInfo = reader.getStripes();

//        int colNumber = 1;
//        String[] colNames = {""};


        StructObjectInspector insp = (StructObjectInspector) reader.getObjectInspector();
        List<StructField> allColInfo = (List<StructField>) insp.getAllStructFieldRefs();    // get info of all cols
        int colNumber = allColInfo.size();
        String[] colNames = new String[colNumber];
        for (int index = 0; index < colNumber; index++) {
            colNames[index] = allColInfo.get(index).getFieldName();
        }


        boolean[] toInclude = new boolean[colNumber+1];   // array must equal to number of column types plus 1
        Arrays.fill(toInclude, true);

        int totalRows = (int) reader.getNumberOfRows();
        long[] col1Method1 = new long[totalRows];
        long[] col1Method2 = new long[totalRows];
        int row_offset = 0;

        VectorizedRowBatch batch = null;

        // access ORC content with method 2.
        RecordReader perStripe = null;     // RecordReader that will scan entire file
        long totalRows2 = 0;
        for (StripeInformation eachStrip:stripesInfo) {
            boolean done = false;
            totalRows2 = totalRows2 + eachStrip.getNumberOfRows();
            perStripe = reader.rows(eachStrip.getOffset(), eachStrip.getDataLength(), toInclude, null, colNames);

            batch = perStripe.nextBatch(batch);

            while (!done) {
                ColumnVector[] dataVectors = batch.cols;

//                long[] temp_time = ((TimestampColumnVector) dataVectors[0]).time;
//                int[] temp_time_nanos = ((TimestampColumnVector) dataVectors[0]).nanos;
                byte[][] temp2 = ((BytesColumnVector) dataVectors[0]).vector;
//                HiveDecimalWritable[] temp2 = ((DecimalColumnVector) dataVectors[0]).vector;
                long[] temp = ((LongColumnVector) dataVectors[0]).vector;



                for (int r=0; r < temp.length; r++) {
                    if (row_offset >= totalRows2) {
                   //     System.out.println("**** Busting out of the max row!");
                        done = true;
                        break;
                    } else {
                        col1Method2[row_offset] = temp[r];
                    }
                    row_offset++;
                }
                if (!done)
                    batch = perStripe.nextBatch(batch);

            }
            perStripe.close();      // release resource
        }

        row_offset = 0;

        RecordReader scanEntireFile = reader.rows(toInclude);     // RecordReader that will scan entire file


        // also try to get info on each
        batch = scanEntireFile.nextBatch(null);
        boolean done = false;
        while (!done) {
            ColumnVector[] dataVectors = batch.cols;    // vectors contains columns of data

            // only want the first column
            long[] temp = ((LongColumnVector) dataVectors[0]).vector;
            for (int r=0; r < temp.length; r++) {
                if (row_offset >= totalRows) {
           //         System.out.println("**** Busting out of the max row!");
                    done = true;
                    break;
                } else {
                    col1Method1[row_offset] = temp[r];
                    row_offset++;
                }

            }
            if (!done)
                batch = scanEntireFile.nextBatch(batch);
        }

        // read a portion of the file.  Need info from stripes
        scanEntireFile.close();

        // compare values read by both methods to make sure they are right
        for (int index = 0; index < totalRows; index++) {
            if (col1Method1[index] != col1Method2[index]) {
                System.out.println("Two methods of getting data do not match!");
                System.out.println("Method 1: "+col1Method1[index]+".  Method 2: "+col1Method2[index]);
            }
        }


        System.out.println("Done!");

    }
}
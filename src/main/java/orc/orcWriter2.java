package orc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.orc.TypeDescription;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import java.io.IOException;

//import org.apache.crunch;
/*
 * This file is written to test and see if we can actually generate a ORC file.
 * This code is copied from https://orc.apache.org/docs/core-java.html
 */

public class orcWriter2 {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
        String typeStr = "struct<x:int,y:int>";
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
        ObjectInspector inspector = OrcStruct.createObjectInspector(typeInfo);

        Path tempPath = new Path("/Users/wendcywong/temp/test.orc");
        Writer writer = OrcFile.createWriter(tempPath,
                OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));
//        Writer writer = OrcFile.createWriter(new Path("my-file.orc"),
//                OrcFile.writerOptions(conf).schema(schema));
//
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        LongColumnVector y = (LongColumnVector) batch.cols[1];
        for(int r=0; r < 10000; ++r) {
            int row = batch.size++;
            x.vector[row] = r;
            y.vector[row] = r * 3;
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
   //             writer.addRowBatch(batch);
                batch.reset();
            }
        }
        writer.close();
    }
}

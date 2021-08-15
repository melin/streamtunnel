package com.github.dzlog.writer;

import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.kafka.LogEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;

/**
 * @author melin 2021/7/27 4:15 下午
 */
public class ParquetFileWriter extends AbstractFileWriter {

    protected SimpleGroupFactory groupFactory;

    protected ParquetWriter<Group> writer;

    public ParquetFileWriter(SimpleGroupFactory groupFactory, Configuration configuration,
                             LogCollectConfig collectConfig, String hivePartition) {
        super(configuration, collectConfig, hivePartition);

        this.groupFactory = groupFactory;
        try {
            this.writer = ExampleParquetWriter.builder(this.getHdfsPath())
                    .withConf(configuration)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .withCompressionCodec(CompressionCodecName.ZSTD).build();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void write(LogEvent logEvent) throws IOException {
        Group group = groupFactory.newGroup();

        group.append("collect_time", logEvent.getReceivedTime());
        group.append("message", Binary.fromConstantByteBuffer(logEvent.getMsgByteBuffer()));
        writer.write(group);
    }

    @Override
    public void close() throws IOException {

    }
}

package com.github.dzlog.writer;

import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.kafka.LogEvent;
import com.github.dzlog.util.TimeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.springframework.data.util.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author melin 2021/7/27 4:15 下午
 */
public class ParquetFileWriter extends AbstractFileWriter {

    protected SimpleGroupFactory groupFactory;

    protected ParquetWriter<Group> writer;

    private byte[] timestampBuffer = new byte[12];

    public ParquetFileWriter(SimpleGroupFactory groupFactory, Configuration configuration,
                             LogCollectConfig collectConfig, String hivePartition) {
        super(collectConfig, hivePartition);

        this.groupFactory = groupFactory;
        try {
            String localFile = this.getLocalFile();
            Path locaPath = new Path(localFile);
            this.writer = ExampleParquetWriter.builder(locaPath)
                    .withConf(configuration)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .withCompressionCodec(CompressionCodecName.ZSTD).build();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void write(LogEvent logEvent) throws IOException {
        Group group = groupFactory.newGroup();

        Pair<Integer, Long> pair = TimeUtils.toJulianDay(logEvent.getReceivedTime() * 1000L);
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(pair.getSecond()).putInt(pair.getFirst());

        group.append("collect_time", Binary.fromReusedByteArray(timestampBuffer));
        group.append("message", Binary.fromConstantByteBuffer(logEvent.getMsgByteBuffer()));
        writer.write(group);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}

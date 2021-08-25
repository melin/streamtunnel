package com.github.dzlog.util;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin
 */
public class HdfsUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

	private static final Logger TROUBLE_LOGGER = LoggerFactory.getLogger("troubleLogger");

	public static void renameFile(FileSystem fileSystem, Path sourcePath, Path distPath) {
		try {
			fileSystem.rename(sourcePath, distPath);
		} catch (Exception e) {
			TROUBLE_LOGGER.error("rename file {} error: {}", sourcePath, e.getMessage());
			throw new RuntimeException("rename file " + sourcePath + " error: " + e.getMessage());
		}
	}

	public static void mkdirs(Configuration conf, Path path) {
		try {
			FileSystem fs = path.getFileSystem(conf);
			fs.mkdirs(path);
		} catch (Exception e) {
			LOGGER.error("mkdir path {} error:{}", path, e.getMessage());
		}
	}

	public static Boolean createFlagFile(Configuration conf, Path path) {
		try {
			FileSystem fs = path.getFileSystem(conf);
			if (!fs.exists(path)) {
				int time = RandomUtils.nextInt(0, 200);
				TimeUnit.MILLISECONDS.sleep(time);

				if (!fs.exists(path)) {
					fs.mkdirs(path);
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		} catch (Exception e) {
			TROUBLE_LOGGER.error("create flag file {} error:{}", path, e.getMessage());
			return false;
		}
	}

	public static void deleteFile(Configuration conf, Path path) {
		try {
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
		} catch (Exception e) {
			LOGGER.error("delete file {} error:{}", path, e.getMessage());
		}
	}

	public static void putLocalFile(Configuration conf, Path src, Path dst) throws IOException {
			FileSystem fs = dst.getFileSystem(conf);

			fs.copyFromLocalFile(true, true, src, dst);
	}

	public static Boolean isPathExist(Configuration conf, Path path) {
		try {
			FileSystem fs = FileSystem.get(conf);
			return fs.exists(path);
		} catch (Exception e) {
			return false;
		}
	}

	public static Boolean isEmptyPartition(Configuration configuration, String dir) {
		try {
			Path dirPath = new Path(dir);
			FileSystem fs = dirPath.getFileSystem(configuration);
			if (!fs.exists(dirPath)) {
				return false;
			}

			int fileNum = 0;
			for (FileStatus status : fs.listStatus(dirPath)) {
				String fileName = status.getPath().getName();
				if (status.isFile() && !fileName.startsWith(".")) {
					fileNum++;
				}
			}
			return fileNum == 0;
		} catch (Exception e) {
			LOGGER.error("get partition {} error {}", dir, e.getMessage());
			return false;
		}
	}

	public static Triple<Long, Long, Long> getPartitionStatus(Configuration configuration, String dir) {
		try {
			Path dirPath = new Path(dir);
			FileSystem fs = dirPath.getFileSystem(configuration);
			if (!fs.exists(dirPath)) {
				return null;
			}

			long fileSize = 0L, numRows = 0L;
			FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> {
				try {
					return fs.isFile(path) && !path.getName().startsWith(".");
				} catch (Exception e) {
					return false;
				}
			});
			long fileCount = fileStatuses.length;
			for (FileStatus status : fileStatuses) {
				fileSize += status.getLen();
			}

			List<FileStatus> fileStatusList = Arrays.asList(fileStatuses);

			List<Footer> footers = ParquetFileReader.readAllFootersInParallel(configuration,
					fileStatusList, false);

			for (Footer f : footers) {
				List<BlockMetaData> blockMetaDataList = f.getParquetMetadata().getBlocks();
				for (BlockMetaData b : blockMetaDataList) {
					numRows += b.getRowCount();
				}
			}

			return Triple.of(fileCount, fileSize, numRows);
		} catch (Exception e) {
			LOGGER.error("get partition {} status error: {}", dir, e.getMessage());
			return null;
		}
	}
}

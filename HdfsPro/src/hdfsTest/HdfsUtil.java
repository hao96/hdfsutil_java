package hdfsTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 通过hdfs 的 java api 对hdfs进行操作
 * @author hao
 *
 */
public class HdfsUtil {
	
	private FileSystem fs;
	private static Configuration conf;
	
	public HdfsUtil(){
		try {
			conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.0.200:9000");
			//默认是 3  3个副本
			//conf.set("fs.replication", "2");
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public FileSystem getFileSystem() {
		return fs;
	}
	/**
	 * 判断文件是否存在
	 * @param path
	 * @return
	 */
	public boolean IsFileExist(String path) {
		boolean b = false;
		try {
			 b = fs.exists(new Path(path));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return b;
	}

	/**
	 * 在hdfs中创建目录
	 * @param dir
	 * @return
	 */
	public boolean makeDir(String dir) {
		boolean b = false;
		try {
			b = fs.mkdirs(new Path(dir));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return b;
	}
	
	/**
	 * 删除文件
	 * @param path
	 * @return
	 */
	public boolean deleteFile(String path) {
		boolean b = false;
		try {
			fs.delete(new Path(path), true);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return b;
	}
	
	/**
	 * 将源地址的文件 拷贝到 目的地址
	 * @param souPath
	 * @param direPath
	 */
	public void insertFile(String souPath,String direPath) {
		try {
			FileInputStream fis = new FileInputStream(new File(souPath));
			FSDataOutputStream fsos = fs.create(new Path(direPath));
			IOUtils.copyBytes(fis, fsos, 1024);
			System.out.println("插入成功！");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 查找该路径下的所有文件（不包括子目录）
	 * @param path
	 * @return
	 */
	public FileStatus[] searchFile(String path) {
		FileStatus[] status = null;
		try {
			status = fs.listStatus(new Path(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return status;
	}
}















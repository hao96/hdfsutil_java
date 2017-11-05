package hdfsTest;

import org.apache.hadoop.fs.FileStatus;

public class Test {

	public static void main(String[] args) {

		HdfsUtil hdfsUtil = new HdfsUtil();
		
		//判断文件夹存在否
/*		if(hdfsUtil.IsFileExist("/yh")) {
			System.out.println("存在");
		}else {
			System.out.println("不存在");
		}*/
		//创建文件夹
/*		if(hdfsUtil.IsFileExist("/test")) {
			System.out.println("该文件夹已存在");
		}else {
			if(hdfsUtil.makeDir("/test")) {
				System.out.println("创建成功！");
			}
		}*/
		
		//删除文件
		/*if(hdfsUtil.IsFileExist("/yh/马士兵hadoop2.7入门_01.mp4")) {
			hdfsUtil.deleteFile("/yh/马士兵hadoop2.7入门_01.mp4");
		}else {
			System.out.println("该文件不存在，删除失败");
			}*/
		
	
		//插入文件
		/*hdfsUtil.insertFile("/home/hao/test.txt", "/yh/test.txt");*/
		
		
		//查看文件信息
		FileStatus[] status = hdfsUtil.searchFile("/yh");
		for(FileStatus f : status) {
			System.out.println("文件位置："+f.getPath());
			System.out.println("文件权限：" + f.getPermission());
			System.out.println("文件副本数：" + f.getReplication());
		}
	}

}

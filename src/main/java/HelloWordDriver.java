import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HelloWordDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        System.out.println(appResponse);

        //设置应用程序提交上下文
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        //appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName("hello world app");

        //为应用程序主机设置本地资源
        //本地文件或档案根据需要
        //在这种情况下，应用程序主文件的jar文件是本地资源的一部分
        Map<String, LocalResource> localResources = new HashMap<>();

        List<String> commands = new ArrayList<>();

        Configuration fsConf = new Configuration();
        fsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        fsConf.set("fs.defaultFS","hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(fsConf);

        addToLocalResources(fs, "/Users/freud.wy/IdeaProjects/yarn_test/src/main/resources/helloworld.sh", "helloworld.sh", appId.toString(),
                localResources, null);

        addToLocalResources(fs, "/Users/freud.wy/IdeaProjects/yarn_test/target/yarn_test-1.0-SNAPSHOT-jar-with-dependencies.jar", "yarn_test-1.0-SNAPSHOT-jar-with-dependencies.jar", appId.toString(),
                localResources, null);

        commands.add("java -jar yarn_test-1.0-SNAPSHOT-jar-with-dependencies.jar " + appId.toString());
        //commands.add("pwd");
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderror");
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, new HashMap<>(), commands, null, null, null);
        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        Resource capability = Resource.newInstance(1024, 1);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Priority.newInstance(1);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue("default");

        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);

        yarnClient.submitApplication(appContext);

//        // Get application report for the appId we are interested in
       ApplicationReport report = yarnClient.getApplicationReport(appId);

    }

    private static void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = "helloworld" + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }
}


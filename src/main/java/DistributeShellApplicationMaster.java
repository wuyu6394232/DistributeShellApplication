import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributeShellApplicationMaster {
    public static void main(String[] args) throws IOException, YarnException {
        String appId = args[0];
        System.out.println("appId:" + appId);

        Map<String, String> envs = System.getenv();
        String containerIdString =
                envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());


        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();


        NMCallbackHandler containerListener = new NMCallbackHandler();
        NMClientAsync nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(new Configuration());
        nmClientAsync.start();

        RMCallbackHandler allocListener = new RMCallbackHandler(nmClientAsync, appId);
        AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        allocListener.setAMRMClientAsync(amRMClient);
        amRMClient.init(new Configuration());
        amRMClient.start();


        // Register self with ResourceManager
        // This will start heartbeating to the RM
        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(appMasterHostname, 10000,
                        "");


        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();

        // A resource ask cannot exceed the max.
        //if (containerMemory > maxMem) {
//            LOG.info("Container memory specified above max threshold of cluster."
//                    + " Using max value." + ", specified=" + containerMemory + ", max="
//                    + maxMem);
//            containerMemory = maxMem;
//        }
//
//        if (containerVirtualCores > maxVCores) {
//            LOG.info("Container virtual cores specified above max threshold of  cluster."
//                    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
//                    + maxVCores);
//            containerVirtualCores = maxVCores;
//        }
//        List<Container> previousAMRunningContainers =
//                response.getContainersFromPreviousAttempts();
////        LOG.info("Received " + previousAMRunningContainers.size()
//                + " previous AM's running containers on AM registration.");


        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();


        int numTotalContainersToRequest =
                2 - previousAMRunningContainers.size();
        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for
        // containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).
        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
        }


    }

    private static AMRMClient.ContainerRequest setupContainerAskForRM() {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Priority.newInstance(1);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements
        Resource capability = Resource.newInstance(1024,
                1);

        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
                pri);

        return request;
    }

    private static class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        private NMClientAsync nmClientAsync;
        private String appId;
        private AMRMClientAsync amrmClientAsync;
        AtomicInteger numCompletedContainers = new AtomicInteger(0);

        public RMCallbackHandler( NMClientAsync nmClientAsync, String appId) {
            this.nmClientAsync = nmClientAsync;
            this.appId = appId;

        }

        public void setAMRMClientAsync(AMRMClientAsync amrmClientAsync) {
            this.amrmClientAsync = amrmClientAsync;
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            System.out.println("onContainersCompleted");
            for (ContainerStatus status : statuses) {
                if (ContainerState.COMPLETE.equals(status.getState())) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    int completeValue = numCompletedContainers.incrementAndGet();
                    if (completeValue == 2) {
                        try {
                            amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "hello world success", null);
                        } catch (YarnException ex) {
                            System.err.println("Failed to unregister application:" + ex);
                        } catch (IOException e) {
                            System.err.println("Failed to unregister application:" + e);
                        }
                        amrmClientAsync.stop();
                        nmClientAsync.stop();
                        System.exit(0);
                    }
                }
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {

//            AtomicInteger numAllocatedContainers;
//            numAllocatedContainers.addAndGet(allocatedContainers.size());
            System.out.println("onContainersAllocated");
            for (Container container : allocatedContainers) {
                System.out.println("contain id:" + container.getId().toString());
                LaunchContainerRunnable runnableLaunchContainer =
                        new LaunchContainerRunnable(nmClientAsync, container, appId);
                Thread launchThread = new Thread(runnableLaunchContainer);

                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                //launchThreads.add(launchThread);
                launchThread.start();
                //numAllocatedContainers.incrementAndGet();
            }
        }

        @Override
        public void onShutdownRequest() {

        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {

        }

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            float progress = (float) numCompletedContainers.get()
                    / 2;
            System.out.println("progress:" + progress);
            return progress;
        }

        @Override
        public void onError(Throwable e) {

        }
    }

    private static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        }

        @Override
        public void onContainerStopped(ContainerId containerId) {

        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {

        }
    }

    public static class LaunchContainerRunnable implements Runnable {

        private NMClientAsync nmClientAsync;
        private Container container;
        private String appId;

        public LaunchContainerRunnable(NMClientAsync nmClientAsync, Container container, String appId) {
            this.nmClientAsync = nmClientAsync;
            this.container = container;
            this.appId = appId;
        }


        @Override
        public void run() {
            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<>(5);

            // Set executable command
            vargs.add("./helloworld.sh");
//        // Set shell script path
//        if (!scriptPath.isEmpty()) {
//            vargs.add(Shell.WINDOWS ? ExecBatScripStringtPath
//                    : ExecShellStringPath);
//        }

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<>();
            commands.add(command.toString());

            // Set up ContainerLaunchContext, setting local resource, environment,
            // command and token for constructor.

            // Note for tokens: Set up tokens for the container too. Today, for normal
            // shell commands, the container in distribute-shell doesn't need any
            // tokens. We are populating them mainly for NodeManagers to be able to
            // download anyfiles in the distributed file-system. The tokens are
            // otherwise also useful in cases, for e.g., when one is running a
            // "hadoop dfs" command inside the distributed shell.
            Configuration fsConf = new Configuration();
            fsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            fsConf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = null;
            try {
                fs = FileSystem.get(fsConf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            FileStatus scFileStatus = null;
            String suffix = "helloworld" + "/" + appId + "/" + "helloworld.sh";
            Path hdfsPath = new Path(fs.getHomeDirectory(), suffix);
            try {
                scFileStatus = fs.getFileStatus(hdfsPath);
            } catch (Exception e) {
                e.printStackTrace();
            }


            LocalResource scRsrc =
                    LocalResource.newInstance(
                            ConverterUtils.getYarnUrlFromURI(hdfsPath.toUri()),
                            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                            scFileStatus.getLen(), scFileStatus.getModificationTime());

            Map<String, LocalResource> localResources = new HashMap<>();
            localResources.put("helloworld.sh", scRsrc);


            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, new HashMap<>(), commands, null, null, null);

            //containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

//    private static void addToLocalResources(FileSystem fs, String fileSrcPath,
//                                            String fileDstPath, String appId, Map<String, LocalResource> localResources,
//                                            String resources) throws IOException {
//        String suffix = "helloworld" + "/" + appId + "/" + fileDstPath;
//        Path dst = new Path(fs.getHomeDirectory(), suffix);
//        if (fileSrcPath == null) {
//            FSDataOutputStream ostream = null;
//            try {
//                ostream = FileSystem
//                        .create(fs, dst, new FsPermission((short) 0710));
//                ostream.writeUTF(resources);
//            } finally {
//                IOUtils.closeQuietly(ostream);
//            }
//        } else {
//            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
//        }
//        FileStatus scFileStatus = fs.getFileStatus(dst);
//        LocalResource scRsrc =
//                LocalResource.newInstance(
//                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
//                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
//                        scFileStatus.getLen(), scFileStatus.getModificationTime());
//        localResources.put(fileDstPath, scRsrc);
//    }
}

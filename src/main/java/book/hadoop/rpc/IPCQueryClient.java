package book.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class IPCQueryClient {
    public static void main(String[] args) {
        try {
            System.out.println("Interface name: " + IPCQueryStatus.class.getName());
            System.out.println("Interface name: " + IPCQueryStatus.class.getMethod("getFileStatus", String.class).getName());

            InetSocketAddress addr = new InetSocketAddress("localhost", IPCQueryServer.IPC_PORT);
            IPCQueryStatus query = (IPCQueryStatus) RPC.getProxy(IPCQueryStatus.class,
                    IPCQueryServer.IPC_VER,
                    addr,
                    new Configuration());
            IPCFileStatus status = query.getFileStatus("/tmp/testIPC");
            System.out.println(status);
            RPC.stopProxy(query);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

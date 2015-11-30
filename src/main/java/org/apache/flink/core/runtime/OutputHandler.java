package org.apache.flink.core.runtime;


import com.google.common.primitives.Bytes;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.zeromq.ZMQ;

public class OutputHandler {

    private final ZMQ.Context context = ZMQ.context(1);
    private final ZMQ.Socket publisher;

    public OutputHandler(String host,int port) {
        publisher = context.socket(ZMQ.PUSH);
        publisher.setSndHWM(0);
        publisher.connect("tcp://" + host + ":" + port);
    }

    public void sendOpen(int taskNumber,
                         int numTasks,
                         byte[] serializer) {
        String open = String.format("OPEN %d %d",
                taskNumber,
                numTasks);
        byte[] msg = Bytes.concat((open + " SER").getBytes(), serializer);
        publisher.send(msg);

    }

    public void sendRecord(byte[] bytes) {
        byte[] msg = Bytes.concat("REC".getBytes(), bytes);
        publisher.send(msg);
    }

    public void sendClose(int taskNumber) {
        String end = String.format("CLOSE %d",
                taskNumber);
        publisher.send(end);
        publisher.close();
        context.term();
    }

}

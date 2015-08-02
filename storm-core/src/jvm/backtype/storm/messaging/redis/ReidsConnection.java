package backtype.storm.messaging.redis;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;

/**
 * Created by ding on 14-8-2.
 */
class ReidsConnection implements IConnection {

    private byte[] queue;
    private Jedis jedis;

    ReidsConnection(byte[] queue, Jedis jedis) {
        this.queue = queue;
        this.jedis = jedis;
    }

    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
       List<TaskMessage> msgs = new ArrayList<TaskMessage>();
        byte[] data = null;
        if (flags == 0) { // block
            while (true) {
                data = jedis.lpop(queue);
                if (data != null) {
                    break;
                } else {
                    Utils.sleep(1);
                }
            }
        } else {
            data = jedis.lpop(queue);
        }
        if (data != null) {
            TaskMessage msg = new TaskMessage(-1, null);
            msg.deserialize(ByteBuffer.wrap(data));
            msgs.add(msg);
        }
        return msgs.iterator();
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage msg = new TaskMessage(taskId, payload);
        List<TaskMessage> wrapper = new ArrayList<TaskMessage>(1);
        wrapper.add(msg);
        send(wrapper.iterator());
    }

    @Override
    public void send(Iterator<TaskMessage> msgs) {
        while (msgs.hasNext()) {
            TaskMessage msg = msgs.next();
            ByteBuffer buf = msg.serialize();
            jedis.rpush(queue, buf.array());
        }
    }

    @Override
    public void close() {
        jedis.disconnect();
    }
}

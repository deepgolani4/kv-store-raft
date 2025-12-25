package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


public class KvRaftClient extends DB {
  public static final String HOST_PROPERTY = "kvraft.host";
  public static final String PORT_PROPERTY = "kvraft.port";
  public static final String FIELD0 = "field0";

  private String host;
  private int port;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    host = props.getProperty(HOST_PROPERTY, "127.0.0.1");
    port = Integer.parseInt(props.getProperty(PORT_PROPERTY, "9000"));
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String resp = send("get:" + key);
    if (resp.startsWith("value:")) {
      String value = resp.substring("value:".length());
      if (fields == null || fields.isEmpty()) {
        result.put(FIELD0, new StringByteIterator(value));
      } else {
        for (String field : fields) {
          result.put(field, new StringByteIterator(value));
        }
      }
      return Status.OK;
    }
    if ("err:key_error".equals(resp)) {
      return Status.NOT_FOUND;
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String value = pickValue(values);
    String resp = send("update:" + key + ":" + value);
    return resp.startsWith("ok:") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String value = pickValue(values);
    String resp = send("put:" + key + ":" + value);
    return resp.startsWith("ok:") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String resp = send("delete:" + key);
    return resp.startsWith("ok:") ? Status.OK : Status.ERROR;
  }

  private String pickValue(Map<String, ByteIterator> values) {
    ByteIterator v = values.get(FIELD0);
    if (v != null) {
      return v.toString();
    }
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      return e.getValue().toString();
    }
    return "";
  }

  private String send(String line) {
    try (Socket socket = new Socket(host, port);
         BufferedWriter writer = new BufferedWriter(
             new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
      writer.write(line);
      writer.write('\n');
      writer.flush();
      String resp = reader.readLine();
      return resp == null ? "" : resp;
    } catch (IOException e) {
      return "err:io";
    }
  }
}

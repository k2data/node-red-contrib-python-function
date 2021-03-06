module.exports = function (RED) {
  var spawn = require("child_process").spawn;
  var util = require("util");

  function indentLines(fnCode, depth) {
    return fnCode
      .split("\n")
      .map((line) => Array(depth).join(" ") + line)
      .join("\n");
  }

  function spawnFn(self) {
    self.child = spawn("python", ["-uc", self.func.code], {
      stdio: ["pipe", "pipe", "pipe", "pipe", "ipc"],
    });
    self.child.stdout.on("data", function (data) {
      self.log(data.toString());
    });
    self.child.stderr.on("data", function (data) {
      self.error(data.toString());
    });
    self.child.on("close", function (exitCode) {
      if (exitCode) {
        self.error(`Python Function process exited with code ${exitCode}`);
        if (self.func.attempts) {
          spawnFn(self);
          self.func.attempts--;
        } else {
          self.error(
            `Function '${self.name}' has failed more than 10 times. Fix it and deploy again`
          );
          self.status({
            fill: "red",
            shape: "dot",
            text: "Stopped, see debug panel",
          });
        }
      }
    });
    self.child.on("message", function (response) {
      switch (response.ctx) {
        case "send":
          sendResults(self, response.msgid, response.value);
          clearCachedMsg(self, response.msgid);
          break;
        case "log":
        case "warn":
        case "error":
        case "status":
          self[response.ctx].apply(self, response.value);
          break;
        default:
          clearCachedMsg(self, response.msgid);
          throw new Error(`Don't know what to do with ${response.ctx}`);
      }
    });
    self.log(`Python function '${self.name}' running on PID ${self.child.pid}`);
    self.status({ fill: "green", shape: "dot", text: "Python" });
  }

  function clearCachedMsg(self, msgid) {
    if (!self.cache || !msgid) return;
    self.cache.delete(msgid);
  }

  function sendResults(self, _msgid, msgs) {
    if (msgs == null) {
      return;
    } else if (!util.isArray(msgs)) {
      msgs = [msgs];
    }
    var msgCount = 0;
    for (var m = 0; m < msgs.length; m++) {
      if (msgs[m]) {
        if (util.isArray(msgs[m])) {
          for (var n = 0; n < msgs[m].length; n++) {
            msgs[m][n]._msgid = _msgid;
            msgCount++;
          }
        } else {
          msgs[m]._msgid = _msgid;
          msgCount++;
        }
      }
    }
    if (msgCount > 0) {
      const msgCache = self.cache?.get(_msgid);
      if (msgCache) {
        // Restore REQ object if it exists.
        if (msgCache.req !== undefined) {
          msgs[0].req = msgCache.req;
        }
        // Restore RES object if it exists.
        if (msgCache.res !== undefined) {
          msgs[0].res = msgCache.res;
        }
      }

      self.send(msgs);
    }
  }

  function PythonFunction(config) {
    var self = this;
    RED.nodes.createNode(self, config);
    self.name = config.name;
    self.cache = new Map();
    self.func = {
      code:
        `
import os
import json
import asyncio
import functools
import concurrent.futures


class Msg(object):
    SEND = 'send'
    LOG = 'log'
    WARN = 'warn'
    ERROR = 'error'
    STATUS = 'status'

    def __init__(self, ctx, value, msgid):
        self.ctx = ctx
        self.value = value
        self.msgid = msgid

    def dumps(self):
        return json.dumps(vars(self)) + "\\n"

    @classmethod
    def loads(cls, json_string):
        return cls(**json.loads(json_string))


class Node(object):
    def __init__(self, msgid, writer, writer_sync):
        self.__msgid = msgid
        self.__writer = writer
        self.__writer_sync = writer_sync

    async def send(self, msg):
        msg = Msg(Msg.SEND, msg, self.__msgid)
        await self.send_to_node(msg)

    def log(self, *args):
        msg = Msg(Msg.LOG, args, self.__msgid)
        self.send_to_node_sync(msg)

    def warn(self, *args):
        msg = Msg(Msg.WARN, args, self.__msgid)
        self.send_to_node_sync(msg)

    def error(self, *args):
        msg = Msg(Msg.ERROR, args, self.__msgid)
        self.send_to_node_sync(msg)

    def status(self, *args):
        msg = Msg(Msg.STATUS, args, self.__msgid)
        self.send_to_node_sync(msg)

    async def send_to_node(self, msg):
        self.__writer.write(msg.dumps().encode('utf-8'))
        await self.__writer.drain()

    def send_to_node_sync(self, msg):
        # ????????????????????????????????????????????????
        # self.__writer_sync.write(msg.dumps().encode('utf-8'))

        self.__writer.write(msg.dumps().encode('utf-8'))
        # ???????????? drain??????????????? drain??????????????????????????? drain???
        # ?????? send_to_node ??? drain
        # https://stackoverflow.com/questions/53779956/why-should-asyncio-streamwriter-drain-be-explicitly-called
        # self.__writer.drain()


def python_function(msg, node):
` +
        indentLines(config.func, 4) +
        `
async def connect_stdin_stdout(loop):
    r_channel = os.fdopen(3, "r+b", buffering=0)
    # w_fd = os.dup(3)
    w_channel = os.fdopen(4, "r+b", buffering=0)
    
    ## Connect reader with pipe
    reader = asyncio.StreamReader(limit=2**22)
    r_protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: r_protocol, r_channel)
    
    ## Connect writer with pipe
    # Method 1:
    # w_protocol = asyncio.StreamReaderProtocol(asyncio.StreamReader())
    # w_transport, _ = await loop.connect_write_pipe(
    #     lambda: w_protocol,
    #     w_channel
    # )
    # writer = asyncio.StreamWriter(w_transport, w_protocol, None, loop)
    # Method 2:
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin,
        w_channel
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)

    return reader, writer, w_channel

async def run_task(msg, node):
    loop = asyncio.get_event_loop()
    python_function_with_msg = functools.partial(python_function, msg, node)

    # executor ??????
    # 1. ?????????????????? thread pool
    # 2. CPU-bound ??????????????????????????????????????? process pool

    # ????????? process pool
    # ??? process pool ????????? node ?????????
    # TypeError: cannot pickle '_io.FileIO' object
    # with concurrent.futures.ProcessPoolExecutor(max_workers=10) as pool:
    #     res_msgs = await loop.run_in_executor(
    #         pool, python_function_with_msg)
    #     await node.send(res_msgs)

    # ????????? thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        res_msgs = await loop.run_in_executor(
            pool, python_function_with_msg)
        await node.send(res_msgs)
  
    # ????????? pathos.multiprocessing

async def main():
    loop = asyncio.get_event_loop()
    reader, writer, writer_sync = await connect_stdin_stdout(loop)
    while True:
        raw_msg = await reader.readline()
        if not raw_msg:
            raise RuntimeError('Received EOF!')
        msg = json.loads(raw_msg)
        msgid = msg["_msgid"]
        node = Node(msgid, writer, writer_sync)

        asyncio.create_task(run_task(msg, node))


asyncio.run(main())
`,
      attempts: 10,
    };

    spawnFn(self);

    self.on("input", function (msg) {
      const msgCache = {};
      // Save REQ object if it exists.
      if (msg.req !== undefined) {
        msgCache.req = msg.req;
      }
      // Save RES object if it exists.
      if (msg.res !== undefined) {
        msgCache.res = msg.res;
      }
      self.cache.set(msg._msgid, msgCache);

      const jsonMsg = serializeMsg(msg);

      // ?????????????????? self.child.send(JSON.parse(jsonMsg))
      // python ??? asyncio ????????????????????????????????????????????? nodejs
      // ??? ipc ?????????????????????????????????????????? pipe ???????????????
      self.child.stdio[3].write(`${jsonMsg}\n`);
    });
    self.on("close", function () {
      self.child.kill();
      self.cache.clear();
    });
  }
  RED.nodes.registerType("python-function", PythonFunction);
};

function serializeMsg(msg) {
  // ?????? payload ?????????????????????????????????
  if (
    msg.req &&
    (msg.payload === msg.req.query || msg.payload === msg.req.body)
  ) {
    msg.payload = { ...msg.payload };
  }

  let cache = [];

  // ???????????????????????????
  // ???????????????https://github.com/douglascrockford/JSON-js/blob/master/cycle.js
  const stringified = JSON.stringify(msg, function (_, value) {
    if (typeof value === "object" && value !== null) {
      if (cache.indexOf(value) !== -1) {
        // Circular reference found, discard key
        return;
      }
      // Store value in our collection
      cache.push(value);
    }
    return value;
  });

  cache = null; // Enable garbage collection

  return stringified;
}

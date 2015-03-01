(ns net.pasdziernik.pytlowac.looper
  (:require [clojure.core.async :refer [chan close! alts!! >!!]]
            [clojure.core.match :refer [match]]
            [clojure.set :refer [map-invert]]
            [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy]
            [zeromq.zmq :as zmq])
  (:import [org.zeromq ZMQ$Poller]
           [java.util.concurrent LinkedBlockingQueue]))


(defn- send! [sock msg]
  (let [msg (if (coll? msg) msg [msg])]
    (loop [[head & tail] msg]
      (let [options (if tail
                      (bit-or zmq/no-block zmq/send-more)
                      zmq/no-block)
            bytes (nippy/freeze head)
            res (zmq/send sock bytes options)]
        (cond
          (= false res) false
          tail (recur tail)
          :else true)))))

(defn- receive-all [sock]
  (let [parts (map nippy/thaw
                   (zmq/receive-all sock))]
    (if (= (count parts) 1)
      (first parts)
      parts)))

(defn- poll [zcontext socks]
  (let [n      (count socks)
        poller (zmq/poller zcontext n)]
    (doseq [sock socks]
      (zmq/register poller sock :pollin))
    (zmq/poll poller)
    ;;Randomly take the first ready socket and its message, to match core.async's alts! behavior
    (->> (shuffle (range n))
         (filter #(zmq/check-poller poller % :pollin))
         first
         (.getSocket ^ZMQ$Poller poller)
         ((juxt receive-all identity)))))

(defn- zmq-looper-fn
  "Runnable fn with blocking loop on zmq sockets.
   Opens/closes zmq sockets according to messages received on `zmq-control-sock`.
   Relays messages from zmq sockets to `async-control-chan`."
  [zcontext ^LinkedBlockingQueue queue zmq-control-sock async-control-chan]
  (fn []
    ;;Socks is a map of string socket-ids to ZeroMQ socket objects (plus a single :control keyword key associated with the thread's control socket).
    (loop [socks {:control zmq-control-sock}]
      (let [[val sock] (poll zcontext (vals socks))
            id (get (map-invert socks) sock)
            ;;Hack coercion  so we can have a pattern match against message from control socket
            val (if (= :control id) (keyword (String. val)) val)]
        (assert (not (nil? id)))
        (match [id val]

               ;;A message indicating there's a message waiting for us to process on the queue.
               [:control :sentinel]
               (let [msg (.take queue)]
                 (match [msg]

                        [[:register sock-id new-sock]]
                        (recur (assoc socks sock-id new-sock))

                        [[:close sock-id]]
                        (do
                          (.close (get socks sock-id))
                          (recur (dissoc socks sock-id)))

                        ;;Send a message out
                        [[sock-id outgoing-message]]
                        (do
                          (when-not (send! (get socks sock-id) outgoing-message)
                            (log/warn "message not sent on sock" sock ", requeing.")
                            (>!! async-control-chan [:retry msg]))
                          (recur socks))

                        ))

               [:control :shutdown]
               (doseq [[_ sock] socks]
                 (zmq/close sock))

               [:control msg]
               (throw (Exception. (str "bad ZMQ control message: " msg)))

               ;;It's an incoming message, send it to the async thread to convey to the application
               [incoming-sock-id msg]
               (do
                 (>!! async-control-chan [incoming-sock-id msg])
                 (recur socks)))))))

(defn- sock-id-for-chan [c pairings]
  (first (for [[id {in :in out :out}] pairings
               :when (#{in out} c)]
           id)))

(defn- command-zmq-thread!
  "Helper used by the core.async thread to relay a command to the ZeroMQ thread.
   Puts message of interest on queue and then sends a sentinel value over zmq-control-sock so that ZeroMQ thread unblocks."
  [zmq-control-sock queue msg]
  (.put ^LinkedBlockingQueue queue msg)
  (send! zmq-control-sock "sentinel"))

(defn- shutdown-pairing!
  "Close ZeroMQ socket with `id` and all associated channels."
  [[sock-id chanmap] zmq-control-sock queue]
  (command-zmq-thread! zmq-control-sock queue
                       [:close sock-id])
  (doseq [[_ c] chanmap]
    (when c (close! c))))

(defn- async-looper-fn
  "Runnable fn with blocking loop on channels.
   Controlled by messages sent over provided `async-control-chan`.
   Sends messages to complementary `zmq-looper` via provided `zmq-control-sock` (assumed to be connected)."
  [queue async-control-chan zmq-control-sock]
  (fn []
    ;; Pairings is a map of string id to {:out chan :in chan} map, where existence of :out and :in depend on the type of ZeroMQ socket.
    (loop [pairings {:control {:in async-control-chan}}]
      (let [in-chans (remove nil? (map :in (vals pairings)))
            [val c] (alts!! in-chans)
            id (sock-id-for-chan c pairings)]

        (match [id val]
               ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
               ;;Control messages

               ;;Register a new socket.
               [:control [:register sock chanmap]]
               (let [sock-id (str (gensym "zmq-"))]
                 (command-zmq-thread! zmq-control-sock queue [:register sock-id sock])
                 (recur (assoc pairings sock-id chanmap)))

               [:control [:retry msg]]
               (do
                 (command-zmq-thread! zmq-control-sock queue msg)
                 (recur pairings))

               ;;Relay a message from ZeroMQ socket to core.async channel.
               [:control [sock-id msg]]
               (let [out (get-in pairings [sock-id :out])]
                 #_(assert out)
                 ;;We have a contract with library consumers that they cannot give us channels that can block, so this >!! won't tie up the async looper.
                 (if out
                   (>!! out msg)
                   (log/fatal "Missing out channel for" sock-id "Message:" msg))
                 (recur pairings))

               ;;The control channel has been closed, close all ZMQ sockets and channels.
               [:control nil]
               (let [opened-pairings (dissoc pairings :control)]

                 (doseq [p opened-pairings]
                   (shutdown-pairing! p zmq-control-sock queue))

                 (send! zmq-control-sock "shutdown")
                 ;;Don't recur...
                 nil)

               [:control msg] (throw (Exception. (str "bad async control message: " msg)))


               ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
               ;;Non-control messages

               ;;The channel was closed, close the corresponding socket.
               [id nil]
               (do
                 (shutdown-pairing! [id (pairings id)] zmq-control-sock queue)
                 (recur (dissoc pairings id)))

               ;;Just convey the message to the ZeroMQ socket.
               [id msg]
               (do
                 (command-zmq-thread! zmq-control-sock queue [id msg])
                 (recur pairings)))))))

(defn- start-daemon [name looper-fn]
  (println "starting " name)
  (doto (Thread. (fn []
                   (log/info "Daemon" name "started")
                   (looper-fn)
                   (log/info "Daemon" name "stopped")))
    (.setName name)
    (.setDaemon true)
    (.start))
  (println "ret"))

(defn start-looper [name zcontext]
  (let [queue (LinkedBlockingQueue. 8)
        async-control-chan (chan)
        addr (str "inproc://" (gensym "zmq-async-"))
        sock-server (zmq/socket zcontext :pair)
        sock-client (zmq/socket zcontext :pair)]
    (zmq/bind sock-server addr)
    (start-daemon (str "ZeroMQ looper " "[" name "]")
                  (zmq-looper-fn zcontext queue sock-server async-control-chan))
    (zmq/connect sock-client addr)
    (start-daemon (str "Core.Async looper " "[" name "]")
                  (async-looper-fn queue async-control-chan sock-client))
    async-control-chan))




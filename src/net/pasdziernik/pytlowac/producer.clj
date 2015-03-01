(ns net.pasdziernik.pytlowac.producer
  (:require [clojure.core.async :refer [chan go-loop <! >! close!]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [taoensso.nippy :as nippy]
            [zeromq.zmq :as zmq]
            [net.pasdziernik.pytlowac.context :refer [register!]])
  (:import [org.zeromq ZMQ$Socket]
           [java.util UUID]))


(defn- connect-socket [socket endpoints]
  (zmq/set-linger socket 0)
  (try
    (doseq [endpoint endpoints]
      (zmq/connect socket endpoint))
    socket
    (catch Exception e
      (zmq/close socket)
      (throw e))))

(defn- pack  [msg]
  [(.getBytes "") (nippy/freeze msg)])

(defn- unpack [msg-frames]
  (if (and (= (count msg-frames) 2)
           (= 0 (count (first msg-frames))))
    (nippy/thaw (second msg-frames))
    (throw (RuntimeException. "Invalid message frames received."))))

(defn- start-producer-loop [out-chan send-chan acks-chan]
  (go-loop []
    ;; TODO: this needs a ctrl-chan to stop?
    (if-let [data (<! out-chan)]
      (let [msg {:id   (UUID/randomUUID)
                 :data data}]
        (println "SEND: " msg)
        (>! send-chan (pack msg))
        (let [ack (unpack (<! acks-chan))]
          (println "ACK: " ack))
        (recur))
      (log/info "Producer loop stopped")))
  (log/info "Producer loop started"))

(defrecord Producer [out-chan endpoints context stop-fn]

  component/Lifecycle

  (start [this]
    (let [sock (-> (zmq/socket (:zcontext context) :dealer)
                   (connect-socket endpoints))
          send-chan (chan)
          acks-chan (chan)]
      (register! context sock send-chan acks-chan)
      (start-producer-loop out-chan send-chan acks-chan)
      (assoc this :stop-fn #(close! send-chan))))

  (stop [this]
    (when stop-fn
      (stop-fn))
    (assoc this :stop-fn nil))

  )

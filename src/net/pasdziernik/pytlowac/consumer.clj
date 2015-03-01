(ns net.pasdziernik.pytlowac.consumer
  (:require [clojure.core.async :refer [chan go-loop <! >! close!]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [taoensso.nippy :as nippy]
            [zeromq.zmq :as zmq]
            [net.pasdziernik.pytlowac.context :refer [register!]])
  (:import [org.zeromq ZMQ$Socket]))


(defn- bind-socket [sock endpoint]
  (zmq/set-linger sock 0)
  (if-not (= (.bind ^ZMQ$Socket sock endpoint) -1)
    sock
    (throw (RuntimeException. (str "Binding zmq socket failed for endpoint: " endpoint)))))

(defn- start-consumer-loop [in-chan recv-chan acks-chan]
  (go-loop []
    (if-let [resp (<! recv-chan)]
      (let [msg (nippy/thaw resp)]
        (println "RECV: " msg)
        (>! acks-chan (nippy/freeze {:ack (:id msg)}))
        (>! in-chan (:data msg))
        (recur))
      (log/info "Consumer loop stopped")))
  (log/info "Consumer loop started"))

(defrecord Consumer [in-chan endpoint context stop-fn]

  component/Lifecycle

  (start [this]
    (let [sock (-> (zmq/socket (:zcontext context) :rep)
                   (bind-socket endpoint))
          recv-chan (chan)
          acks-chan (chan)]
      (register! context sock acks-chan recv-chan)
      (start-consumer-loop in-chan recv-chan acks-chan)
      (assoc this :stop-fn #(close! acks-chan))))

  (stop [this]
    (when stop-fn
      (stop-fn))
    (assoc this :stop-fn nil))

  )


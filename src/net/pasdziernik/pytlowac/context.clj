(ns net.pasdziernik.pytlowac.context
  (:require [clojure.core.async :refer [close!]]
            [com.stuartsierra.component :as component]
            [zeromq.zmq :as zmq]
            [net.pasdziernik.pytlowac.looper :refer [start-looper]]))


(defrecord Context [name zcontext ctrl-chan]

  component/Lifecycle

  (start [this]
    (let [zcontext (zmq/context 1)
          ctrl-chan (start-looper name zcontext)]
      (-> this
          (assoc :zcontext zcontext)
          (assoc :ctrl-chan ctrl-chan))))

  (stop [this]
    (close! (:ctrl-chan this))
    (-> this
        (assoc :zcontext nil)
        (assoc :ctrl-chan nil)))

  )

(defn context [name]
  (Context. name nil nil))

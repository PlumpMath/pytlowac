(ns net.pasdziernik.pytlowac.api
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [net.pasdziernik.pytlowac.context]
            [net.pasdziernik.pytlowac.producer]
            [net.pasdziernik.pytlowac.consumer])
  (:import [net.pasdziernik.pytlowac.context Context]
           [net.pasdziernik.pytlowac.producer Producer]
           [net.pasdziernik.pytlowac.consumer Consumer]))

(defn context [name]
  (Context. name nil nil))

(defn producer [out-chan endpoints]
  (Producer. out-chan endpoints nil nil))

(defn consumer [in-chan endpoint]
  (Consumer. in-chan endpoint nil nil))

(defn get-system [out-chan in-chan]
  (-> (component/system-map
        :context (context "test")
        :producer (producer out-chan ["tcp://127.0.0.1:8888"])
        :consumer (consumer in-chan "tcp://127.0.0.1:8888"))
      (component/system-using
        {:producer {:context :context}
         :consumer {:context :context}})))

(defn simple-test []
  (let [in (async/chan)
        out (async/chan)
        sys (component/start (get-system out in))]
    (async/go (async/>! out {:test 1}))
    (async/<!! in)
    (component/stop sys)))

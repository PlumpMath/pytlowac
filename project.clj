(defproject net.pasdziernik/pytlowac "0.1.0-SNAPSHOT"
  :description "Clojure core.async over JeroMQ"
  :url "https://github.com/mpasdziernik/pytlowac"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/core.match "0.2.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.stuartsierra/component "0.2.2"]
                 [com.taoensso/nippy "2.8.0"]
                 [org.zeromq/cljzmq "0.1.4" :exclusions [org.zeromq/jzmq]]
                 [org.zeromq/jeromq "0.3.4"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.3"]]}
             :test {:dependencies [[midje "1.6.3"]]
                    :plugins [[lein-midje "3.1.3"]]}})

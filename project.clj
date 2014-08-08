(defproject io.mandoline/mandoline-dynamodb "0.1.4"
  :description "DynamoDB backend for Mandoline."
  :license {:name "Apache License, version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :min-lein-version "2.0.0"
  :url
    "https://github.com/TheClimateCorporation/mandoline-dynamodb"
  :mailing-lists
    [{:name "mandoline-users@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-users"
      :post "mandoline-users@googlegroups.com"}
     {:name "mandoline-dev@googlegroups.com"
      :archive "https://groups.google.com/d/forum/mandoline-dev"
      :post "mandoline-dev@googlegroups.com"}]

  :checksum :warn
  :dependencies
    [[org.clojure/clojure "1.5.1"]
     [org.slf4j/slf4j-log4j12 "1.7.2"]
     [log4j "1.2.17"]
     [org.clojure/tools.logging "0.2.6"]
     [org.clojure/core.cache "0.6.3"]
     [org.clojure/core.memoize "0.5.6"]
     [joda-time/joda-time "2.1"]
     [com.taoensso/faraday "1.4.0"]
     [io.mandoline/mandoline-core "0.1.4"]]
  :exclusions [org.clojure/clojure]

  :aliases {"docs" ["marg" "-d" "target"]
            "package" ["do" "clean," "jar"]}

  :plugins [[lein-marginalia "0.7.1"]
            [lein-cloverage "1.0.2"]]
  :test-selectors {:default :unit
                   :integration :integration
                   :local :local
                   :unit :unit
                   :all (constantly true)})
